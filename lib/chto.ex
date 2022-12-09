defmodule Chto do
  @moduledoc false

  def insert_stream(repo, table, rows, opts \\ []) do
    fields = intersperce_map(opts[:fields] || [], ?,, &quote_name/1)
    statement = ["INSERT INTO ", quote_name(table), ?(, fields, ?)]
    repo.query(statement, rows, put_in(opts, [:command], :insert))
  end

  defp intersperce_map([elem], _separator, mapper), do: [mapper.(elem)]

  defp intersperce_map([elem | rest], separator, mapper) do
    [mapper.(elem), separator | intersperce_map(rest, separator, mapper)]
  end

  defp intersperce_map([], _separator, _mapper), do: []

  defp quote_name(name, quoter \\ ?")
  defp quote_name(nil, _), do: []

  defp quote_name(name, quoter) when is_atom(name) do
    name |> Atom.to_string() |> quote_name(quoter)
  end

  defp quote_name(name, quoter) do
    if String.contains?(name, <<quoter>>) do
      raise "bad name #{inspect(name)}"
    end

    wrap_in(name, quoter)
  end

  defp wrap_in(value, nil), do: value
  defp wrap_in(value, wrapper), do: [wrapper, value, wrapper]
end
