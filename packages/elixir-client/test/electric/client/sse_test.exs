defmodule Electric.Client.SSETest do
  use ExUnit.Case, async: true
  alias Electric.Client
  alias Electric.Client.Message.{ControlMessage, ChangeMessage, ResumeMessage}
  alias Electric.Client.SSE.Decoder
  import Plug.Conn

  test "decoder handles every byte boundary and all line endings" do
    for ending <- ["\n", "\r\n", "\r"] do
      input =
        Enum.join(
          [
            ":keepalive",
            "event: ignored",
            "data: {",
            "data: \"name\":\"雪🙂\"}",
            "",
            "data:",
            "",
            "data: incomplete"
          ],
          ending
        )

      for size <- 1..byte_size(input) do
        chunks =
          for index <- 0..(div(byte_size(input), size) - 1),
              do: binary_part(input, index * size, size)

        tail = binary_part(input, length(chunks) * size, rem(byte_size(input), size))

        Enum.reduce(chunks ++ [tail], %Decoder{}, fn chunk, decoder ->
          Decoder.feed(decoder, chunk, &send(self(), {:event, &1}))
        end)

        assert_receive {:event, "{\n\"name\":\"雪🙂\"}"}
        refute_receive {:event, _}, 0
      end
    end
  end

  defp headers(conn, type \\ "text/event-stream") do
    conn
    |> put_resp_header("content-type", type)
    |> put_resp_header("electric-handle", "handle")
    |> put_resp_header("electric-offset", "999_inf")
    |> put_resp_header("electric-cursor", "123")
    |> put_resp_header("electric-schema", ~s({"id":{"type":"int4"}}))
  end

  defp up(lsn), do: %{"headers" => %{"control" => "up-to-date", "global_last_seen_lsn" => lsn}}

  defp row(id),
    do: %{
      "key" => "#{id}",
      "value" => %{"id" => "#{id}"},
      "headers" => %{"operation" => "insert"}
    }

  defp event(msg), do: "data: #{Jason.encode!(msg)}\n\n"

  defp client(bypass, opts \\ []) do
    Client.new!(base_url: "http://localhost:#{bypass.port}", fetch: {Client.Fetch.HTTP, opts})
  end

  defp resume,
    do: %ResumeMessage{shape_handle: "handle", offset: "1_inf", schema: %{id: %{type: "int4"}}}

  test "snapshot then SSE yields a complete batch before closure and cleans up on take" do
    bypass = Bypass.open()
    owner = self()

    Bypass.expect(bypass, fn conn ->
      conn = fetch_query_params(conn)

      if conn.query_params["live_sse"] do
        assert conn.query_params["live"] == "true"
        assert get_req_header(conn, "accept") == ["text/event-stream"]
        {:ok, conn} = conn |> headers() |> send_chunked(200) |> chunk(event(row(2)))
        send(owner, {:partial, self()})

        receive do
          :finish -> :ok
        end

        {:ok, conn} = chunk(conn, event(up(2)))

        receive do
          :close -> :ok
        end

        conn
      else
        refute conn.query_params["live"]
        conn |> headers("application/json") |> resp(200, Jason.encode!([up(1)]))
      end
    end)

    task =
      Task.async(fn -> client(bypass) |> Client.stream("items", live: :sse) |> Enum.take(3) end)

    assert_receive {:partial, server}, 3000
    assert Task.yield(task, 30) == nil
    send(server, :finish)

    assert [%ControlMessage{}, %ChangeMessage{value: %{"id" => 2}}, %ControlMessage{}] =
             Task.await(task)

    send(server, :close)
  end

  test "reconnect uses committed event checkpoint, never response offset" do
    bypass = Bypass.open()
    owner = self()

    Bypass.expect(bypass, fn conn ->
      conn = fetch_query_params(conn)
      offset = conn.query_params["offset"]
      send(owner, {:offset, offset})
      lsn = if offset == "1_inf", do: 2, else: 3
      conn |> headers() |> resp(200, event(up(lsn)))
    end)

    assert [%ControlMessage{}, %ControlMessage{}] =
             client(bypass, request: [retry_delay: 1])
             |> Client.stream("items", live: :sse, resume: resume())
             |> Enum.take(2)

    assert_receive {:offset, "1_inf"}
    assert_receive {:offset, "2_inf"}
  end

  for {name, body} <- [
        {"malformed JSON", "data: {oops}\n\n"},
        {"invalid payload", "data: []\n\n"},
        {"missing checkpoint", "data: {\"headers\":{\"control\":\"up-to-date\"}}\n\n"}
      ] do
    test name do
      bypass = Bypass.open()
      Bypass.expect(bypass, fn conn -> conn |> headers() |> resp(200, unquote(body)) end)

      assert [%Client.Error{}] =
               client(bypass)
               |> Client.stream("items", live: :sse, resume: resume(), errors: :stream)
               |> Enum.to_list()
    end
  end

  test "rejects content type and missing headers in both error modes" do
    for mode <- [:raise, :stream], type <- ["application/json", "text/event-stream"] do
      bypass = Bypass.open()

      Bypass.expect(bypass, fn conn ->
        conn |> put_resp_header("content-type", type) |> resp(200, event(up(2)))
      end)

      stream =
        client(bypass) |> Client.stream("items", live: :sse, resume: resume(), errors: mode)

      if mode == :raise do
        assert_raise Client.Error, fn -> Enum.to_list(stream) end
      else
        assert [%Client.Error{}] = Enum.to_list(stream)
      end
    end
  end

  test "interrupted batch is discarded and recovered through finite requests" do
    bypass = Bypass.open()
    owner = self()

    Bypass.expect(bypass, fn conn ->
      conn = fetch_query_params(conn)
      send(owner, {:request, conn.query_params})

      cond do
        conn.query_params["live_sse"] == "true" and conn.query_params["offset"] == "1_inf" ->
          conn |> headers() |> resp(200, event(row(2)))

        conn.query_params["live_sse"] == nil ->
          assert conn.query_params["offset"] == "1_inf"
          refute conn.query_params["live"]

          conn
          |> headers("application/json")
          |> put_resp_header("electric-offset", "2_inf")
          |> resp(200, Jason.encode!([row(2), up(2)]))

        true ->
          conn |> headers() |> resp(200, event(up(3)))
      end
    end)

    assert [%ChangeMessage{}, %ControlMessage{}, %ControlMessage{}] =
             client(bypass, request: [retry_delay: 1])
             |> Client.stream("items", live: :sse, resume: resume())
             |> Enum.take(3)
  end

  defmodule CustomParser do
    def for_schema(_schema, _opts), do: fn values -> Map.put(values, "parsed", true) end
  end

  test "parser override and tag processing are shared with polling" do
    bypass = Bypass.open()
    tagged = put_in(row(2), ["headers", "tags"], ["tag-a"])

    move_in = %{
      "headers" => %{"event" => "move-in", "patterns" => [%{"pos" => 0, "value" => "tag-a"}]}
    }

    move_out = put_in(move_in, ["headers", "event"], "move-out")

    Bypass.expect_once(bypass, fn conn ->
      conn
      |> headers()
      |> resp(200, Enum.map_join([tagged, up(2), move_in, move_out, up(3)], &event/1))
    end)

    assert [
             %ChangeMessage{value: %{"parsed" => true, "id" => "2"}},
             %ControlMessage{},
             %ChangeMessage{
               headers: %{operation: :delete},
               value: %{"parsed" => true, "id" => "2"}
             },
             %ControlMessage{}
           ] =
             client(bypass)
             |> Client.stream("items", live: :sse, resume: resume(), parser: {CustomParser, []})
             |> Enum.take(4)
  end

  test "stale SSE metadata triggers cache-busted catch-up without yielding stale rows" do
    bypass = Bypass.open()
    client = client(bypass)
    key = Client.ShapeKey.canonical(client.endpoint, %{"table" => "items"})
    Client.ExpiredShapesCache.mark_expired(key, "handle")
    on_exit(fn -> Client.ExpiredShapesCache.clear_handle(key) end)

    Bypass.expect(bypass, fn conn ->
      conn = fetch_query_params(conn)

      if conn.query_params["live_sse"] do
        conn |> headers() |> resp(200, event(row(99)) <> event(up(99)))
      else
        assert conn.query_params["cache-buster"]

        conn
        |> headers("application/json")
        |> put_resp_header("electric-handle", "new")
        |> resp(200, Jason.encode!([row(2), up(2)]))
      end
    end)

    assert [%ChangeMessage{key: "2"}, %ControlMessage{}] =
             client |> Client.stream("items", live: :sse, resume: resume()) |> Enum.take(2)
  end

  test "recovery yields pages before up-to-date and fetches only on demand" do
    bypass = Bypass.open()
    owner = self()

    Bypass.expect(bypass, fn conn ->
      conn = fetch_query_params(conn)

      cond do
        conn.query_params["live_sse"] ->
          conn |> headers() |> resp(200, event(row(99)))

        conn.query_params["offset"] == "1_inf" ->
          conn
          |> headers("application/json")
          |> put_resp_header("electric-offset", "2_0")
          |> resp(200, Jason.encode!([row(2)]))

        true ->
          assert conn.query_params["offset"] == "2_0"
          refute conn.query_params["live"]
          send(owner, :final_page_requested)

          conn |> headers("application/json") |> resp(200, Jason.encode!([up(2)]))
      end
    end)

    stream =
      client(bypass, request: [retry_delay: 1])
      |> Client.stream("items", live: :sse, resume: resume())

    {:suspended, %ChangeMessage{key: "2"}, next} =
      Enumerable.reduce(stream, {:cont, nil}, fn msg, _ -> {:suspend, msg} end)

    refute_receive :final_page_requested, 30
    assert {:suspended, %ControlMessage{control: :up_to_date}, halt} = next.({:cont, nil})
    assert_receive :final_page_requested
    halt.({:halt, nil})
  end

  for transport <- [:poll, :http, :event] do
    @tag timeout: 15_000
    test "repeated 409s terminate through the fast-loop guard after #{transport}" do
      bypass = Bypass.open()
      owner = self()

      Bypass.expect(bypass, fn conn ->
        conn = fetch_query_params(conn)
        send(owner, :requested)

        if conn.query_params["live_sse"] == "true" and unquote(transport) == :event do
          conn
          |> headers()
          |> resp(200, event(%{"headers" => %{"control" => "must-refetch"}}))
        else
          conn
          |> headers("application/json")
          |> put_resp_header("electric-handle", "next")
          |> resp(409, "[]")
        end
      end)

      opts =
        if unquote(transport) == :poll,
          do: [live: true, errors: :stream],
          else: [live: :sse, resume: resume(), errors: :stream]

      messages = client(bypass) |> Client.stream("items", opts) |> Enum.to_list()
      assert %Client.Error{message: message} = List.last(messages)
      assert message =~ "stuck in a fast retry loop"
      assert Enum.any?(messages, &match?(%ControlMessage{control: :must_refetch}, &1))
      assert_receive :requested
    end
  end

  for failure <- [:reset, :error, :rewind] do
    test "recovery handles #{failure} after a page has been delivered" do
      bypass = Bypass.open()
      tagged = put_in(row(2), ["headers", "tags"], ["tag-a"])

      Bypass.expect(bypass, fn conn ->
        conn = fetch_query_params(conn)

        cond do
          conn.query_params["live_sse"] ->
            conn |> headers() |> resp(200, event(row(99)))

          conn.query_params["offset"] == "-1" ->
            move_out = %{
              "headers" => %{
                "event" => "move-out",
                "patterns" => [%{"pos" => 0, "value" => "tag-a"}]
              }
            }

            conn
            |> headers("application/json")
            |> put_resp_header("electric-handle", "new")
            |> resp(200, Jason.encode!([move_out, row(3), up(3)]))

          conn.query_params["offset"] == "1_inf" ->
            conn
            |> headers("application/json")
            |> put_resp_header("electric-offset", "2_0")
            |> resp(200, Jason.encode!([tagged]))

          unquote(failure) == :rewind ->
            conn
            |> headers("application/json")
            |> put_resp_header("electric-offset", "2_0")
            |> resp(200, "[]")

          true ->
            status = if unquote(failure) == :reset, do: 409, else: 403

            conn
            |> headers("application/json")
            |> put_resp_header("electric-handle", "new")
            |> resp(status, Jason.encode!(["denied"]))
        end
      end)

      stream =
        client(bypass, request: [retry_delay: 1])
        |> Client.stream("items", live: :sse, resume: resume(), errors: :stream)

      if unquote(failure) == :error do
        assert [%ChangeMessage{key: "2"}, %Client.Error{message: "denied"}] = Enum.to_list(stream)
      else
        assert [
                 %ChangeMessage{key: "2"},
                 %ControlMessage{control: :must_refetch},
                 %ChangeMessage{key: "3"},
                 %ControlMessage{control: :up_to_date}
               ] = Enum.take(stream, 4)
      end
    end
  end

  test "request timestamps identify the connection across completed batches" do
    bypass = Bypass.open()

    Bypass.expect_once(bypass, fn conn ->
      conn
      |> headers()
      |> resp(200, event(row(2)) <> event(up(2)) <> event(row(3)) <> event(up(3)))
    end)

    messages =
      client(bypass) |> Client.stream("items", live: :sse, resume: resume()) |> Enum.take(4)

    assert [%DateTime{}] = messages |> Enum.map(& &1.request_timestamp) |> Enum.uniq()
  end

  for recovery? <- [false, true] do
    test "worker clears fast-loop tracking at up-to-date with recovery=#{recovery?}" do
      bypass = Bypass.open()

      Bypass.expect(bypass, fn conn ->
        conn = fetch_query_params(conn)

        if conn.query_params["live_sse"] do
          messages = if unquote(recovery?), do: event(row(99)), else: event(up(2))
          conn |> headers() |> resp(200, messages)
        else
          conn
          |> headers("application/json")
          |> put_resp_header("electric-offset", "2_inf")
          |> resp(200, Jason.encode!([row(2), up(2)]))
        end
      end)

      client =
        client(bypass, request: [retry_delay: 1])
        |> Client.merge_params(%{"table" => "items"})

      state = %{
        Client.ShapeState.from_resume(resume())
        | fast_loop_consecutive_count: 2,
          value_mapper_fun: fn values -> values end
      }

      worker = Client.SSE.start(client, state, :default)

      try do
        assert {:ok, _, completed} = Client.SSE.next(worker)
        assert completed.up_to_date?
        assert completed.recent_requests == []
        assert completed.fast_loop_consecutive_count == 0
      after
        Client.SSE.close(worker)
      end
    end
  end

  test "HTTP error construction does not interpret 409 as a state transition" do
    for status <- [403, 409, 500] do
      response = %Client.Fetch.Response{status: status, body: ["denied"]}

      assert %Client.Error{message: "denied", resp: ^response} =
               Client.Protocol.response_error(response)
    end
  end

  test "decoder handles large lines and dispatches coalesced events synchronously" do
    value = String.duplicate("x", 100_000)
    input = "data: " <> value <> "\r\ndata: tail\r\n\r\ndata: next\n\n"
    parent = self()

    task =
      Task.async(fn ->
        Decoder.feed(%Decoder{}, input, fn event ->
          send(parent, {:decoded, event})

          receive do
            :continue -> :ok
          end
        end)
      end)

    expected = value <> "\ntail"
    assert_receive {:decoded, ^expected}
    refute_receive {:decoded, "next"}, 30
    send(task.pid, :continue)
    assert_receive {:decoded, "next"}
    send(task.pid, :continue)
    assert %Decoder{line: [], data: []} = Task.await(task)
  end

  test "permanent HTTP errors preserve ordinary response bodies" do
    bypass = Bypass.open()

    Bypass.expect_once(bypass, fn conn ->
      conn
      |> put_resp_header("content-type", "application/json")
      |> resp(403, Jason.encode!(["denied"]))
    end)

    assert [%Client.Error{message: "denied", resp: %{status: 403}}] =
             client(bypass)
             |> Client.stream("items", live: :sse, resume: resume(), errors: :stream)
             |> Enum.to_list()
  end

  test "identical up-to-date checkpoints do not reset the failure budget" do
    bypass = Bypass.open()
    Bypass.expect(bypass, fn conn -> conn |> headers() |> resp(200, event(up(1))) end)

    messages =
      client(bypass, timeout: 1, request: [retry_delay: 600])
      |> Client.stream("items", live: :sse, resume: resume(), errors: :stream)
      |> Enum.to_list()

    assert %Client.Error{message: "SSE retry timeout exhausted"} = List.last(messages)
  end
end
