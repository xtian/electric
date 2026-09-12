defmodule Electric.Client.SSEIntegrationTest do
  use ExUnit.Case, async: false
  import Support.DbSetup
  alias Electric.Client
  alias Electric.Client.Message.{ChangeMessage, ControlMessage}

  # Close one real HTTP response at a batch boundary to exercise resumption.
  defmodule DisconnectOnce do
    @behaviour Client.Fetch
    def validate_opts(opts), do: {:ok, opts}
    def fetch(request, _opts), do: Client.Fetch.HTTP.fetch(request, [])

    def stream(request, opts, emit) do
      send(opts[:owner], {:sse_request, request.offset})
      first? = not Process.get(:integration_connected, false)
      Process.put(:integration_connected, true)
      Process.put(:integration_decoder, %Client.SSE.Decoder{})

      Client.Fetch.HTTP.stream(request, [], fn part ->
        if match?({:data, _}, part) do
          {:data, data} = part

          decoder =
            Client.SSE.Decoder.feed(Process.get(:integration_decoder), data, fn event ->
              if get_in(Jason.decode!(event), ["headers", "control"]) == "up-to-date" do
                Process.put(:integration_batch, true)
              end
            end)

          Process.put(:integration_decoder, decoder)
        end

        action = emit.(part)
        if first? and Process.get(:integration_batch), do: :halt, else: action
      end)
    end
  end

  setup :with_unique_table

  @tag timeout: 30_000
  test "snapshot, SSE writes, checkpoint reconnect and truncate recovery", ctx do
    {:ok, first} = insert_item(ctx)
    owner = self()

    client =
      Client.new!(
        base_url: Application.fetch_env!(:electric_client, :electric_url),
        fetch: {DisconnectOnce, owner: owner, request: [retry_delay: 1]}
      )

    consumer =
      start_supervised!(
        {Task,
         fn ->
           client
           |> Client.stream(ctx.tablename, live: :sse)
           |> Enum.each(&send(owner, {:message, &1}))
         end}
      )

    assert_receive {:message, %ChangeMessage{value: %{"id" => ^first}}}, 10_000
    assert_receive {:message, %ControlMessage{control: :up_to_date}}, 10_000
    assert_receive {:sse_request, _}, 10_000

    {:ok, {second, same_transaction}} =
      with_transaction(ctx, fn tx ->
        {:ok, second} = insert_item(tx)
        {:ok, same_transaction} = insert_item(tx)
        {second, same_transaction}
      end)

    assert_receive {:message, %ChangeMessage{value: %{"id" => ^second}}}, 10_000
    assert_receive {:message, %ChangeMessage{value: %{"id" => ^same_transaction}}}, 10_000

    assert_receive {:message, %ControlMessage{control: :up_to_date, global_last_seen_lsn: lsn}},
                   10_000

    checkpoint = "#{lsn}_inf"
    assert_receive {:sse_request, ^checkpoint}, 10_000
    {:ok, after_reconnect} = insert_item(ctx)
    assert_receive {:message, %ChangeMessage{value: %{"id" => ^after_reconnect}}}, 10_000
    assert_receive {:message, %ControlMessage{control: :up_to_date}}, 10_000
    refute_receive {:message, %ChangeMessage{value: %{"id" => ^second}}}, 100
    refute_receive {:message, %ChangeMessage{value: %{"id" => ^same_transaction}}}, 100
    Postgrex.query!(ctx.db_conn, "TRUNCATE TABLE \"#{ctx.tablename}\"", [])
    assert_receive {:message, %ControlMessage{control: :must_refetch}}, 10_000
    assert_receive {:message, %ControlMessage{control: :up_to_date}}, 10_000
    {:ok, third} = insert_item(ctx)
    assert_receive {:message, %ChangeMessage{value: %{"id" => ^third}}}, 10_000
    Process.exit(consumer, :kill)
  end
end
