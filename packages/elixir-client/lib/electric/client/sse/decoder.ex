defmodule Electric.Client.SSE.Decoder do
  @moduledoc false
  defstruct line: [], data: [], cr?: false

  # Scan binary fragments, not codepoints: UTF-8 can span transport chunks.
  # Dispatch immediately so a coalesced chunk cannot bypass batch backpressure.
  def feed(state, chunk, emit), do: scan(chunk, state, emit)

  defp scan(<<>>, state, _emit), do: state

  defp scan(<<10, rest::binary>>, %{cr?: true} = state, emit),
    do: scan(rest, %{state | cr?: false}, emit)

  defp scan(chunk, state, emit) do
    case :binary.match(chunk, ["\r", "\n"]) do
      {index, 1} ->
        fragment = binary_part(chunk, 0, index)
        ending = :binary.at(chunk, index)
        rest = binary_part(chunk, index + 1, byte_size(chunk) - index - 1)
        value = [fragment | state.line] |> Enum.reverse() |> IO.iodata_to_binary()
        state = line(%{state | line: [], cr?: ending == 13}, value, emit)
        scan(rest, state, emit)

      :nomatch ->
        # Do not retain a large transport chunk for a small unfinished line.
        %{state | line: [:binary.copy(chunk) | state.line], cr?: false}
    end
  end

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

    %{state | data: [:binary.copy(value) | state.data]}
  end

  defp line(state, "data", _emit), do: %{state | data: ["" | state.data]}
  defp line(state, _line, _emit), do: state
end
