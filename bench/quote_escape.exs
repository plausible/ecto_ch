defmodule QuoteEscapeBench do
  def current(value, quoter) do
    value
    |> IO.iodata_to_binary()
    |> :binary.replace("\\", "\\\\", [:global])
    |> :binary.replace(<<quoter>>, "\\" <> <<quoter>>, [:global])
  end

  def one_pass_replace(value, quoter) do
    value = IO.iodata_to_binary(value)

    :binary.replace(value, ["\\", <<quoter>>], "\\", [
      :global,
      {:insert_replaced, 1}
    ])
  end

  def skip_and_slice(value, quoter) do
    value = IO.iodata_to_binary(value)
    pattern = :binary.compile_pattern(["\\", <<quoter>>])

    value
    |> do_skip_and_slice(pattern)
    |> IO.iodata_to_binary()
  end

  defp do_skip_and_slice(value, pattern) do
    case :binary.match(value, pattern) do
      :nomatch ->
        value

      {position, 1} ->
        <<plain::binary-size(^position), escaped, rest::binary>> = value
        [plain, ?\\, escaped | do_skip_and_slice(rest, pattern)]
    end
  end
end

inputs = %{
  "6B plain" => {"events", ?"},
  "12B one quote" => {~s[event"name], ?"},
  "64B plain" => {String.duplicate("abcdefgh", 8), ?"},
  "64B sparse" =>
    {String.duplicate("abcdefgh", 3) <>
       ~s[\"] <> String.duplicate("abcdefgh", 4) <> "abcdef", ?"},
  "64B dense" => {String.duplicate(~s[a\"b], 16), ?"},
  "4KB plain" => {String.duplicate("abcdefgh", 512), ?"},
  "4KB dense" => {String.duplicate(~s[a\"b], 1024), ?"}
}

Enum.each(inputs, fn {_name, {value, quoter}} ->
  expected = QuoteEscapeBench.current(value, quoter)

  unless QuoteEscapeBench.one_pass_replace(value, quoter) == expected and
           QuoteEscapeBench.skip_and_slice(value, quoter) == expected do
    raise "implementations differ"
  end
end)

Benchee.run(
  %{
    "current two-pass replace" => fn {value, quoter} ->
      QuoteEscapeBench.current(value, quoter)
    end,
    "one-pass replace" => fn {value, quoter} ->
      QuoteEscapeBench.one_pass_replace(value, quoter)
    end,
    "skip and slice" => fn {value, quoter} ->
      QuoteEscapeBench.skip_and_slice(value, quoter)
    end
  },
  inputs: inputs,
  warmup: 1,
  time: 2,
  memory_time: 1,
  reduction_time: 1
)
