defmodule Electric.Client.Poll do
  @moduledoc """
  Poll-based API for fetching shape changes.

  This module provides explicit request-response semantics for fetching
  changes from Electric, as an alternative to the streaming API.

  ## Usage

      # Create initial state
      state = ShapeState.new()

      # Make a polling request
      case Poll.request(client, state) do
        {:ok, messages, new_state} ->
          # Process messages, use new_state for next poll
          ...

        {:must_refetch, messages, new_state} ->
          # Shape was reset, clear local state and process messages
          ...

        {:error, error} ->
          # Handle error
          ...
      end

  ## Behavior

  - First request (when `up_to_date?: false`): Makes a non-live request to get initial snapshot
  - Subsequent requests (when `up_to_date?: true`): Makes a live request that long-polls until changes arrive
  - Handles synthetic deletes from move-out events
  - Returns updated state for the next request
  """

  alias Electric.Client
  alias Electric.Client.ShapeState

  @type poll_result ::
          {:ok, [Client.message()], ShapeState.t()}
          | {:must_refetch, [Client.message()], ShapeState.t()}
          | {:error, Client.Error.t()}

  @doc """
  Make a single polling request to fetch shape changes.

  ## Arguments

    * `client` - The Electric client
    * `shape` - The shape definition (or a client pre-configured for a shape)
    * `state` - The current polling state (use `ShapeState.new()` for initial request)
    * `opts` - Options:
      * `:replica` - `:default` or `:full` (default: `:default`)

  ## Returns

    * `{:ok, messages, new_state}` - Success, messages received
    * `{:must_refetch, messages, new_state}` - Shape was reset (409), state has been cleared
    * `{:error, error}` - Error occurred

  ## Examples

      state = ShapeState.new()
      {:ok, messages, state} = Poll.request(client, state, replica: :full)

      # Process messages...

      # Poll again for more changes
      {:ok, messages, state} = Poll.request(client, state, replica: :full)
  """
  @spec request(Client.t(), ShapeState.t(), keyword()) :: poll_result()
  def request(%Client{} = client, %ShapeState{} = state, opts \\ []) do
    Electric.Client.Protocol.request(client, state, opts)
  end
end
