defmodule Chto do
  @moduledoc false

  def insert_stream(repo, table, rows, opts \\ []) do
    fields =
      case opts[:fields] do
        [_ | _] = fields -> [?(, intersperce_map(fields, ?,, &quote_name/1), ?)]
        _none -> []
      end

    statement = ["INSERT INTO ", quote_name(table) | fields]
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

    [quoter, name, quoter]
  end
end
