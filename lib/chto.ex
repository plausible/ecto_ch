defmodule Chto do
  @moduledoc false

  @spec insert_stream(module, String.t(), Enumerable.t(), Keyword.t()) ::
          {:ok, count :: non_neg_integer} | {:error, Exception.t()}
  def insert_stream(repo, table, rows, opts \\ []) do
    fields =
      case opts[:fields] do
        [_ | _] = fields -> [?(, intersperce_map(fields, ?,, &quote_name/1), ?)]
        _none -> []
      end

    statement = ["INSERT INTO ", quote_name(table) | fields]
    opts = put_in(opts, [:command], :insert)

    with {:ok, %{num_rows: num_rows}} <- repo.query(statement, rows, opts) do
      {:ok, num_rows}
    end
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
