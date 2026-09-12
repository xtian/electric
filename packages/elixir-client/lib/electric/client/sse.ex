defmodule Electric.Client.SSE do
  @moduledoc false
  alias Electric.Client
  alias Electric.Client.{Fetch, Protocol, ShapeKey, ShapeState}
  alias Electric.Client.SSE.Decoder

  # One worker per enumeration, never registered in the shared request pool.
  def start(client, state, replica) do
    owner = self()

    {:ok, pid} =
      DynamicSupervisor.start_child(
        Electric.Client.RequestSupervisor,
        %{
          id: make_ref(),
          restart: :temporary,
          start:
            {Task, :start_link,
             [
               fn ->
                 worker = self()

                 spawn(fn ->
                   owner_ref = Process.monitor(owner)
                   worker_ref = Process.monitor(worker)

                   receive do
                     {:DOWN, ^owner_ref, _, _, _} -> Process.exit(worker, :kill)
                     {:DOWN, ^worker_ref, _, _, _} -> :ok
                   end
                 end)

                 demand()

                 try do
                   run(client, state, replica, owner)
                 rescue
                   error -> send(owner, {worker, {:error, wrap_error(error)}})
                 end
               end
             ]}
        }
      )

    {pid, Process.monitor(pid)}
  end

  def next({pid, ref}) do
    send(pid, :next)

    receive do
      {^pid, result} ->
        result

      {:DOWN, ^ref, :process, ^pid, reason} ->
        {:error, %Client.Error{message: "SSE worker stopped", resp: reason}}
    end
  end

  def close(nil), do: :ok

  def close({pid, ref}) do
    Process.demonitor(ref, [:flush])
    closed = Process.monitor(pid)
    Process.exit(pid, :kill)

    receive do
      {:DOWN, ^closed, :process, ^pid, _} -> :ok
    end

    flush(pid)
  end

  defp flush(pid) do
    receive do
      {^pid, _} -> flush(pid)
    after
      0 -> :ok
    end
  end

  defp demand do
    receive do
      :next -> :ok
    end
  end

  defp deliver(owner, result) do
    send(owner, {self(), result})
    demand()
  end

  defp run(client, state, replica, owner) do
    {fetcher, opts} = client.fetch

    unless Code.ensure_loaded?(fetcher) and function_exported?(fetcher, :stream, 3) do
      raise Client.Error, message: "SSE is unsupported by fetch backend #{inspect(fetcher)}"
    end

    # Req's synchronous callback owns this worker process. Keep protocol state
    # here so callbacks can commit batches while the outer request is blocked.
    Process.put(__MODULE__, %{
      state: state,
      pending: [],
      response: nil,
      decoder: %Decoder{},
      error_body: [],
      progress: false
    })

    loop(client, replica, owner, opts, System.monotonic_time(:millisecond), 0)
  end

  defp loop(client, replica, owner, opts, failure_start, attempts) do
    ctx = Process.get(__MODULE__)
    key = ShapeKey.canonical(client.endpoint, client.params)
    request = Protocol.build_request(client, ctx.state, replica, key)

    request = %{
      request
      | live: :sse,
        headers: Map.put(request.headers, "accept", "text/event-stream")
    }

    request = Client.authenticate_request(client, request)
    {fetcher, _} = client.fetch
    started = System.monotonic_time(:millisecond)

    Process.put(__MODULE__, %{
      ctx
      | response: nil,
        decoder: %Decoder{},
        pending: [],
        error_body: [],
        progress: false
    })

    result =
      try do
        fetcher.stream(request, opts, &emit(&1, client, key, owner))
      catch
        :throw, {:sse_reset, result} -> {:reset, result}
        :throw, {:sse_stale, state} -> {:stale, state}
      end

    ctx = Process.get(__MODULE__)

    result =
      if match?(%Fetch.Response{status: 409}, ctx.response) do
        {:reset, Protocol.handle_response({:error, ctx.response}, client, ctx.state, key)}
      else
        result
      end

    case result do
      {:stale, state} ->
        state = catch_up(client, %{state | up_to_date?: false}, replica, owner)
        Process.put(__MODULE__, %{ctx | state: state})
        loop(client, replica, owner, opts, failure_start, attempts)

      {:reset, reset} ->
        {:must_refetch, _, state} = reset
        deliver(owner, reset)
        state = catch_up(client, state, replica, owner)
        Process.put(__MODULE__, %{ctx | state: state})
        loop(client, replica, owner, opts, now(), 0)

      _ ->
        error = response_error(ctx, result)

        if error && not transient?(error, opts) do
          raise wrap_error(error)
        end

        healthy = ctx.progress or now() - started >= 60_000
        failure_start = if healthy, do: now(), else: failure_start
        attempts = if healthy, do: 0, else: attempts
        delay = retry_delay(opts, attempts)
        timeout = Keyword.get(opts, :timeout, 300)

        if timeout != :infinity and now() - failure_start + delay > timeout * 1000 do
          raise Client.Error, message: "SSE retry timeout exhausted", resp: error
        end

        Process.sleep(delay)
        # A partial frame is also an interrupted batch. Re-fetch from the last
        # committed checkpoint, yielding finite catch-up pages with backpressure.
        state =
          if ctx.pending != [] or ctx.decoder.line != [] or ctx.decoder.data != [] do
            catch_up(client, %{ctx.state | up_to_date?: false}, replica, owner)
          else
            ctx.state
          end

        recovered? = state.offset != ctx.state.offset
        Process.put(__MODULE__, %{ctx | state: state})

        loop(
          client,
          replica,
          owner,
          opts,
          if(recovered?, do: now(), else: failure_start),
          if(recovered?, do: 0, else: attempts + 1)
        )
    end
  end

  defp emit({:response, resp}, _client, key, _owner) do
    ctx = Process.get(__MODULE__)

    if resp.status in 200..299 do
      type = resp.headers |> Map.get("content-type", []) |> List.first()

      unless is_binary(type) and
               String.downcase(String.trim(hd(String.split(type, ";")))) == "text/event-stream" do
        raise Client.Error, message: "Expected SSE content-type text/event-stream", resp: resp
      end

      Protocol.validate_headers!(resp, ctx.state)

      case Protocol.validate_handle(resp, ctx.state, key) do
        :ok -> :ok
        {:stale_retry, state} -> throw({:sse_stale, state})
        {:error, error} -> raise error
      end
    end

    Process.put(__MODULE__, %{ctx | response: resp})
    :cont
  end

  defp emit({:data, data}, client, key, owner) do
    ctx = Process.get(__MODULE__)

    if is_nil(ctx.response) do
      raise Client.Error,
        message: "Fetch.stream/3 must emit response metadata before data"
    end

    if ctx.response.status in 200..299 do
      decoder = Decoder.feed(ctx.decoder, data, &event(&1, client, key, owner))
      Process.put(__MODULE__, %{Process.get(__MODULE__) | decoder: decoder})
    else
      Process.put(__MODULE__, %{ctx | error_body: [data | ctx.error_body]})
    end

    :cont
  end

  defp event(data, client, key, owner) do
    msg = Jason.decode!(data)
    validate_message!(msg)
    ctx = Process.get(__MODULE__)

    case msg do
      %{"headers" => %{"control" => "must-refetch"}} ->
        resp = %{ctx.response | status: 409, shape_handle: nil}
        throw({:sse_reset, Protocol.handle_response({:error, resp}, client, ctx.state, key)})

      %{"headers" => %{"control" => "up-to-date", "global_last_seen_lsn" => lsn}} ->
        offset = checkpoint!(lsn)
        resp = %{ctx.response | body: Enum.reverse([msg | ctx.pending]), last_offset: offset}
        result = Protocol.handle_response(resp, client, ctx.state, key)

        case result do
          {:ok, _, state} ->
            state = ShapeState.clear_fast_loop(state)
            {:ok, messages, _} = result
            result = {:ok, messages, state}

            Process.put(__MODULE__, %{
              ctx
              | state: state,
                pending: [],
                progress: ctx.progress or state.offset != ctx.state.offset
            })

            deliver(owner, result)

          {:stale_retry, state} ->
            throw({:sse_stale, state})

          {:error, error} ->
            raise error
        end

      %{"headers" => %{"control" => "up-to-date"}} ->
        raise Client.Error, message: "SSE up-to-date is missing checkpoint metadata"

      _ ->
        Process.put(__MODULE__, %{ctx | pending: [msg | ctx.pending]})
    end
  end

  defp validate_message!(%{"headers" => %{"control" => c}})
       when c in ["up-to-date", "must-refetch", "snapshot-end"], do: :ok

  defp validate_message!(%{"headers" => %{"operation" => op}, "key" => key, "value" => value})
       when op in ["insert", "update", "delete"] and is_binary(key) and is_map(value), do: :ok

  defp validate_message!(%{"headers" => %{"event" => event, "patterns" => patterns}})
       when event in ["move-in", "move-out"] and is_list(patterns), do: :ok

  defp validate_message!(_), do: raise(Client.Error, message: "Invalid SSE message payload")

  # The server emits up-to-date only after all operations through this LSN.
  # Resume after the whole transaction, not after its first operation (L_0).
  defp checkpoint!(lsn) when is_integer(lsn) and lsn >= 0, do: "#{lsn}_inf"

  defp checkpoint!(lsn) when is_binary(lsn) do
    case Integer.parse(lsn) do
      {n, ""} when n >= 0 -> checkpoint!(n)
      _ -> raise Client.Error, message: "Invalid SSE checkpoint"
    end
  end

  defp checkpoint!(_), do: raise(Client.Error, message: "Invalid SSE checkpoint")

  defp catch_up(client, state, replica, owner) do
    state = check_catch_up_loop(state, owner)

    case Protocol.request(client, state, replica: replica) do
      {:ok, messages, state} ->
        state = if state.up_to_date?, do: ShapeState.clear_fast_loop(state), else: state
        deliver(owner, {:ok, messages, state})

        if state.up_to_date? do
          state
        else
          catch_up(client, state, replica, owner)
        end

      {:must_refetch, _, state} = result ->
        deliver(owner, result)
        catch_up(client, state, replica, owner)

      {:stale_retry, state} ->
        catch_up(client, state, replica, owner)

      {:error, error} ->
        raise error
    end
  end

  defp check_catch_up_loop(state, owner) do
    case ShapeState.check_fast_loop(state) do
      {:ok, state} ->
        state

      {:backoff, delay, checked} ->
        checked =
          if checked.fast_loop_consecutive_count == 1 do
            # The guard rewinds to a fresh snapshot. Pages may already have
            # reached the consumer, so reset its view and our tag index together.
            reset = ShapeState.reset(checked, nil)

            message = %Client.Message.ControlMessage{
              control: :must_refetch,
              handle: nil,
              request_timestamp: DateTime.utc_now()
            }

            deliver(owner, {:must_refetch, [message], reset})
            reset
          else
            checked
          end

        if delay > 0, do: Process.sleep(delay)
        checked

      {:error, message} ->
        raise Client.Error, message: message
    end
  end

  defp response_error(ctx, result) do
    case ctx.response do
      %Fetch.Response{status: status} = resp when status not in 200..299 ->
        {:http, %{resp | body: decode_error(ctx.error_body)}}

      _ ->
        case result do
          :ok -> nil
          {:error, error} -> error
        end
    end
  end

  defp decode_error(parts) do
    data = parts |> Enum.reverse() |> IO.iodata_to_binary()

    case Jason.decode(data) do
      {:ok, body} -> body
      _ -> data
    end
  end

  defp transient?({:http, resp}, opts),
    do:
      Fetch.HTTP.transient?(
        %Req.Response{status: resp.status, headers: resp.headers, body: resp.body},
        Keyword.get(opts, :is_transient_fun, &Fetch.HTTP.transient_response?/1)
      )

  defp transient?(error, opts),
    do:
      Fetch.HTTP.transient?(
        error,
        Keyword.get(opts, :is_transient_fun, &Fetch.HTTP.transient_response?/1)
      )

  defp retry_delay(opts, n) do
    case opts
         |> Keyword.get(:request, [])
         |> Keyword.get(:retry_delay, &Fetch.HTTP.retry_delay/1) do
      fun when is_function(fun, 1) -> fun.(min(n, 16))
      delay when is_integer(delay) -> delay
    end
  end

  defp wrap_error(%Client.Error{} = error), do: error

  defp wrap_error({:http, resp}), do: Protocol.response_error(resp)

  defp wrap_error(error), do: %Client.Error{message: "Unable to consume SSE data", resp: error}
  defp now, do: System.monotonic_time(:millisecond)
end
