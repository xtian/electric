# ElectricSQL Elixir client

An Elixir client for [ElectricSQL](https://electric-sql.com).

Electric is a sync engine that allows you to sync
[little subsets](https://electric-sql.com/docs/guides/shapes)
of data from Postgres into local apps and services. This client
allows you to sync data from Electric into Elixir applications.

## Installation

```elixir
def deps do
  [
    {:electric_client, "~> 0.1.0"}
  ]
end
```

## Usage

See the [Documentation](https://hexdocs.pm/electric_client).

## Server-Sent Events (HTTP)

```elixir
client = Electric.Client.new!(base_url: "http://localhost:3000")
Electric.Client.stream(client, "todos", live: :sse)
|> Enum.each(&IO.inspect/1)
```

The initial snapshot uses ordinary HTTP requests. Once it is up to date, each
enumeration opens its own streaming request with `live=true&live_sse=true` and
`Accept: text/event-stream`. The default `live: true` still uses long polling;
`live: false` still stops after the snapshot.

SSE messages are buffered until `up-to-date`, then yielded individually in order.
An incomplete batch can grow in memory; completed batches wait for consumer
demand. Halting enumeration or a reducer failure closes the connection.
Suspending retains the connection and pauses consumption; halt an abandoned
continuation to release it. Consumer death also closes the connection.

Reconnects authenticate again and resume from the last completed batch's Electric
checkpoint. Interrupted batches are discarded and recovered through non-live
requests before SSE resumes. Keepalives refresh `receive_timeout` (milliseconds
in the HTTP fetcher's `request` options); `timeout` (seconds) limits repeated
connection failures, not the lifetime of a healthy stream.

Proxies must forward Electric headers and query parameters, allow
`text/event-stream`, flush response chunks without buffering, and permit
long-lived responses and keepalives. SSE is supported by the HTTP adapter;
`Embedded` and other fetchers without `stream/3` return `Electric.Client.Error`
according to `errors: :raise | :stream`. There is no automatic polling fallback.

## Testing

[Run Electric and Postgres](https://electric-sql.com/docs/guides/installation).

Define `DATABASE_URL` and `ELECTRIC_URL` as env vars. Or see the defaults in `config/runtime.exs`.

Then run:

```sh
mix test
```
