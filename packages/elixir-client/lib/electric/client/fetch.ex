defmodule Electric.Client.Fetch do
  alias Electric.Client.Fetch.{Request, Response}
  alias Electric.Client

  @callback validate_opts(keyword()) :: {:ok, keyword()} | {:error, term()}
  @callback fetch(Request.t(), keyword()) :: {:ok, Response.t()} | {:error, Response.t() | term()}

  @doc """
  Stream response metadata followed by raw body chunks on a dedicated request.
  The synchronous emitter applies backpressure and may halt the connection.
  """
  @callback stream(Request.t(), keyword(), ({:response, Response.t()} | {:data, binary()} ->
                                              :cont | :halt)) ::
              :ok | {:error, term()}
  @optional_callbacks stream: 3

  @behaviour Electric.Client.Fetch.Pool

  def request(client, request, opts \\ [])

  @impl Electric.Client.Fetch.Pool
  def request(%Client{} = client, %Request{} = request, _opts) do
    %{pool: {module, opts}} = client
    apply(module, :request, [client, request, opts])
  end
end
