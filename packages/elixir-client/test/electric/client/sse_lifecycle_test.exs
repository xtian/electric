defmodule Electric.Client.SSELifecycleTest do
  use ExUnit.Case, async: true
  alias Electric.Client
  alias Electric.Client.Message.{ControlMessage, ChangeMessage, ResumeMessage}
  import Plug.Conn

  defmodule ObservingHTTP do
    @behaviour Client.Fetch
    def validate_opts(opts), do: {:ok, opts}

    def fetch(request, opts),
      do: Client.Fetch.HTTP.fetch(request, Keyword.delete(opts, :observer))

    def stream(request, opts, emit) do
      send(opts[:observer], {:worker, self()})
      Client.Fetch.HTTP.stream(request, Keyword.delete(opts, :observer), emit)
    end
  end

  defmodule Unsupported do
    @behaviour Client.Fetch
    def validate_opts(opts), do: {:ok, opts}
    def fetch(_, _), do: raise("must not poll")
  end

  defmodule Authenticator do
    def authenticate_request(request, owner) do
      token = Integer.to_string(System.unique_integer([:positive]))
      send(owner, {:authenticated, token})
      %{request | authenticated: true, headers: Map.put(request.headers, "authorization", token)}
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

  defp client(bypass, opts \\ []),
    do: Client.new!(base_url: "http://localhost:#{bypass.port}", fetch: {Client.Fetch.HTTP, opts})

  defp resume,
    do: %ResumeMessage{shape_handle: "handle", offset: "1_inf", schema: %{id: %{type: "int4"}}}

  defp stream(client, opts \\ []),
    do: Client.stream(client, "items", Keyword.merge([live: :sse, resume: resume()], opts))

  defp observed_stream(bypass) do
    Client.new!(
      base_url: "http://localhost:#{bypass.port}",
      fetch: {ObservingHTTP, observer: self()}
    )
    |> stream()
  end

  test "unsupported backends report both error modes without polling" do
    client = Client.new!(base_url: "http://localhost:1", fetch: {Unsupported, []})
    assert_raise Client.Error, ~r/unsupported/, fn -> client |> stream() |> Enum.to_list() end
    assert [%Client.Error{message: message}] = client |> stream(errors: :stream) |> Enum.to_list()
    assert message =~ "unsupported"
  end

  test "suspension pauses coalesced batches; continuation and explicit halt clean up" do
    bypass = Bypass.open()

    Bypass.expect_once(bypass, fn conn ->
      conn |> headers() |> resp(200, event(up(2)) <> event(up(3)))
    end)

    {:suspended, 2, continuation} =
      Enumerable.reduce(observed_stream(bypass), {:cont, nil}, fn msg, _ ->
        {:suspend, msg.global_last_seen_lsn}
      end)

    assert_receive {:worker, worker}
    ref = Process.monitor(worker)
    assert Process.alive?(worker)
    refute_receive {^worker, _}, 30
    assert {:suspended, 3, halt} = continuation.({:cont, nil})
    assert {:halted, :done} = halt.({:halt, :done})
    assert_receive {:DOWN, ^ref, _, _, _}
  end

  for kind <- [:raise, :throw] do
    test "reducer #{kind} closes worker" do
      bypass = Bypass.open()
      Bypass.expect_once(bypass, fn conn -> conn |> headers() |> resp(200, event(up(2))) end)
      stream = observed_stream(bypass)

      if unquote(kind) == :raise do
        assert_raise RuntimeError, "reducer failed", fn ->
          Enum.each(stream, fn _ -> raise "reducer failed" end)
        end
      else
        assert catch_throw(Enum.each(stream, fn _ -> throw(:failed) end)) == :failed
      end

      assert_receive {:worker, worker}
      refute Process.alive?(worker)
    end
  end

  test "consumer death closes a suspended worker" do
    bypass = Bypass.open()
    Bypass.expect_once(bypass, fn conn -> conn |> headers() |> resp(200, event(up(2))) end)
    stream = observed_stream(bypass)
    owner = self()

    consumer =
      spawn(fn ->
        result = Enumerable.reduce(stream, {:cont, nil}, fn _, _ -> {:suspend, nil} end)
        send(owner, {:suspended, result})

        receive do
          :never -> :ok
        end
      end)

    assert_receive {:worker, worker}, 3000
    assert_receive {:suspended, _}, 3000
    ref = Process.monitor(worker)
    Process.exit(consumer, :kill)
    assert_receive {:DOWN, ^ref, _, _, _}, 3000
  end

  test "enumerations of one stream use independent connections" do
    bypass = Bypass.open()
    Bypass.expect(bypass, fn conn -> conn |> headers() |> resp(200, event(up(2))) end)
    stream = observed_stream(bypass)

    {:suspended, _, first} =
      Enumerable.reduce(stream, {:cont, nil}, fn _, _ -> {:suspend, nil} end)

    {:suspended, _, second} =
      Enumerable.reduce(stream, {:cont, nil}, fn _, _ -> {:suspend, nil} end)

    assert_receive {:worker, one}
    assert_receive {:worker, two}
    assert one != two
    first.({:halt, nil})
    second.({:halt, nil})
    refute Process.alive?(one)
    refute Process.alive?(two)
  end

  test "authenticates each reconnect and preserves configured headers and Finch pool" do
    bypass = Bypass.open()
    owner = self()
    start_supervised!({Finch, name: __MODULE__.Finch})

    Bypass.expect(bypass, fn conn ->
      conn = fetch_query_params(conn)
      send(owner, {:authorization, get_req_header(conn, "authorization")})
      assert get_req_header(conn, "x-custom") == ["value"]
      lsn = if conn.query_params["offset"] == "1_inf", do: 2, else: 3
      conn |> headers() |> resp(200, event(up(lsn)))
    end)

    client =
      client(bypass,
        headers: [{"x-custom", "value"}],
        request: [finch: __MODULE__.Finch, retry_delay: 1]
      )

    client = %{client | authenticator: {Authenticator, self()}}
    assert length(Enum.take(stream(client), 2)) == 2
    assert_receive {:authenticated, first}
    assert_receive {:authorization, [^first]}
    assert_receive {:authenticated, second}
    assert_receive {:authorization, [^second]}
    assert first != second
  end

  test "keepalives refresh the inactivity timeout" do
    bypass = Bypass.open()

    Bypass.expect_once(bypass, fn conn ->
      conn = conn |> headers() |> send_chunked(200)

      conn =
        Enum.reduce(1..8, conn, fn _, conn ->
          Process.sleep(20)
          {:ok, conn} = chunk(conn, ":keepalive\n\n")
          conn
        end)

      {:ok, conn} = chunk(conn, event(up(2)))
      conn
    end)

    assert [%ControlMessage{}] =
             client(bypass, request: [receive_timeout: 80]) |> stream() |> Enum.take(1)
  end

  test "nonprogressing closure and transient HTTP errors exhaust the retry budget" do
    for status <- [200, 503] do
      bypass = Bypass.open()
      Bypass.expect(bypass, fn conn -> conn |> headers() |> resp(status, ":keepalive\n\n") end)

      assert [%Client.Error{message: "SSE retry timeout exhausted"}] =
               client(bypass, timeout: 1, request: [retry_delay: 600])
               |> stream(errors: :stream)
               |> Enum.to_list()
    end
  end

  for reset <- [:http, :event] do
    test "#{reset} reset clears state and fetches a new snapshot" do
      bypass = Bypass.open()

      Bypass.expect(bypass, fn conn ->
        conn = fetch_query_params(conn)

        if conn.query_params["live_sse"] do
          if unquote(reset) == :http do
            conn
            |> headers("application/json")
            |> put_resp_header("electric-handle", "next")
            |> resp(409, "[]")
          else
            conn
            |> headers()
            |> resp(200, event(row(99)) <> event(%{"headers" => %{"control" => "must-refetch"}}))
          end
        else
          assert conn.query_params["offset"] == "-1"
          assert conn.query_params["cache-buster"]
          assert conn.query_params["expired_handle"] == "handle"

          conn
          |> headers("application/json")
          |> put_resp_header("electric-handle", "next")
          |> resp(200, Jason.encode!([row(3), up(3)]))
        end
      end)

      assert [
               %ControlMessage{control: :must_refetch},
               %ChangeMessage{key: "3"},
               %ControlMessage{control: :up_to_date}
             ] =
               client(bypass) |> stream() |> Enum.take(3)
    end
  end
end
