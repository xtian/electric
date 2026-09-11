defmodule Electric.Client.SSE.Decoder do
  @moduledoc false
  defstruct line: [], data: [], cr?: false

  # Scan bytes, not codepoints: UTF-8 characters can span transport chunks.
  # Dispatch immediately so a coalesced chunk cannot bypass batch backpressure.
  def feed(state, chunk, emit), do: scan(chunk, state, emit)

  defp scan(<<>>, state, _emit), do: state

  defp scan(<<10, rest::binary>>, %{cr?: true} = state, emit),
    do: scan(rest, %{state | cr?: false}, emit)

  defp scan(<<c, rest::binary>>, state, emit) when c in [10, 13] do
    line = state.line |> Enum.reverse() |> IO.iodata_to_binary()
    state = line(%{state | line: [], cr?: c == 13}, line, emit)
    scan(rest, state, emit)
  end

  defp scan(<<c, rest::binary>>, state, emit),
    do: scan(rest, %{state | line: [<<c>> | state.line], cr?: false}, emit)

  defp line(%{data: []} = state, "", _emit), do: state

  defp line(state, "", emit) do
    data = state.data |> Enum.reverse() |> Enum.join("\n")
    if data != "", do: emit.(data)
    %{state | data: []}
  end

  defp line(state, "data:" <> value, _emit) do
    value =
      case value do
        " " <> rest -> rest
        rest -> rest
      end

    %{state | data: [value | state.data]}
  end

  defp line(state, "data", _emit), do: %{state | data: ["" | state.data]}
  defp line(state, _line, _emit), do: state
end
