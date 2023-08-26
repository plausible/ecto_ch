defmodule Ecto.Adapters.ClickHouse.Migration do
  @moduledoc false
  alias Ecto.Migration.{Table, Reference, Index, Constraint}
  @conn Ecto.Adapters.ClickHouse.Connection

  @dialyzer :no_improper_lists

  @spec execute_ddl(Ecto.Adapter.Migration.command()) :: iodata
  def execute_ddl({command, %Table{} = table, columns})
      when command in [:create, :create_if_not_exists] do
    %Table{prefix: prefix, name: name, options: options, engine: engine} = table

    create =
      case command do
        :create -> "CREATE TABLE "
        :create_if_not_exists -> "CREATE TABLE IF NOT EXISTS "
      end

    engine = engine || Application.get_env(:ecto_ch, :default_table_engine) || "TinyLog"
    pk = if engine in ["TinyLog", "Memory"], do: [], else: pk_definition(columns)
    columns = @conn.intersperse_map(columns, ?,, &column_definition/1)
    options = options_expr(options)

    [
      [
        create,
        @conn.quote_table(prefix, name),
        ?(,
        columns,
        pk,
        ?),
        " ENGINE=",
        engine,
        options
      ]
    ]
  end

  def execute_ddl({command, %Table{} = table, _mode}) when command in [:drop, :drop_if_exists] do
    drop =
      case command do
        :drop_if_exists -> "DROP TABLE IF EXISTS "
        :drop -> "DROP TABLE "
      end

    [
      [drop | @conn.quote_table(table.prefix, table.name)]
    ]
  end

  def execute_ddl({:alter, %Table{} = table, changes}) do
    Enum.map(changes, fn change ->
      [
        "ALTER TABLE ",
        @conn.quote_table(table.prefix, table.name),
        ?\s | column_change(change)
      ]
    end)
  end

  def execute_ddl({command, %Index{} = index})
      when command in [:create, :create_if_not_exists] do
    if index.unique do
      raise ArgumentError, "ClickHouse does not support UNIQUE INDEX"
    end

    # TODO or :using?
    type = index.options[:type]
    type || raise ArgumentError, "expected :type in options, got: #{inspect(type)}"

    granularity = index.options[:granularity]

    granularity ||
      raise ArgumentError, "expected :granularity in options, got: #{inspect(granularity)}"

    fields = @conn.intersperse_map(index.columns, ?,, &index_expr/1)

    create =
      case command do
        :create -> "CREATE INDEX "
        :create_if_not_exists -> "CREATE INDEX IF NOT EXISTS "
      end

    [
      [
        create,
        @conn.quote_name(index.name),
        " ON ",
        @conn.quote_table(index.prefix, index.table),
        " (",
        fields,
        ") TYPE ",
        to_string(type),
        " GRANULARITY "
        | to_string(granularity)
      ]
    ]
  end

  def execute_ddl({command, %Index{} = index, _mode})
      when command in [:drop, :drop_if_exists] do
    drop =
      case command do
        :drop -> "DROP INDEX "
        :drop_if_exists -> "DROP INDEX IF EXISTS "
      end

    [[drop | @conn.quote_table(index.prefix, index.name)]]
  end

  def execute_ddl({:create, %Constraint{} = constraint}) do
    table_name = @conn.quote_table(constraint.prefix, constraint.table)
    constraint_expr = constraint |> constraint_expr() |> Enum.join("")

    [["ALTER TABLE ", table_name, " ADD ", constraint_expr]]
  end

  def execute_ddl({:create_if_not_exists, %Constraint{} = _constraint}) do
    raise "TODO"
  end

  def execute_ddl({command, %Constraint{} = _constraint, _mode})
      when command in [:drop, :drop_if_exists] do
    raise "TODO"
  end

  def execute_ddl({:rename, %Table{} = current_table, %Table{} = new_table}) do
    [
      [
        "RENAME TABLE ",
        @conn.quote_table(current_table.prefix, current_table.name),
        " TO ",
        @conn.quote_table(new_table.prefix, new_table.name)
      ]
    ]
  end

  def execute_ddl({:rename, %Table{} = table, column, name}) do
    [
      [
        "ALTER TABLE ",
        @conn.quote_table(table.prefix, table.name),
        " RENAME COLUMN ",
        @conn.quote_name(column),
        " TO ",
        @conn.quote_name(name)
      ]
    ]
  end

  def execute_ddl(string) when is_binary(string) do
    [string]
  end

  def execute_ddl(list) when is_list(list) do
    raise ArgumentError, "ClickHouse adapter does not support lists in execute_ddl"
  end

  defp pk_definition(columns) do
    pk_columns =
      Enum.filter(columns, fn {_, _, _, opts} ->
        case Keyword.get(opts, :primary_key, false) do
          true = t -> t
          false = f -> f
        end
      end)

    case pk_columns do
      [] = empty ->
        empty

      pk_columns ->
        pk_expr =
          pk_columns
          |> Enum.map(fn {_, name, type, _} ->
            if type in [:serial, :bigserial] do
              raise ArgumentError,
                    "type #{inspect(type)} is not supported as ClickHouse does not support AUTOINCREMENT"
            end

            @conn.quote_name(name)
          end)
          |> Enum.intersperse(?,)

        [",PRIMARY KEY (", pk_expr, ?)]
    end
  end

  defp column_definition({:add, _name, %Reference{}, _opts}) do
    raise ArgumentError, "ClickHouse does not support FOREIGN KEY"
  end

  defp column_definition({:add, name, type, opts}) do
    [
      @conn.quote_name(name),
      ?\s,
      column_type(type)
      | column_options(type, opts)
    ]
  end

  # TODO collate support?
  defp column_options(type, opts) do
    default = Keyword.fetch(opts, :default)
    null = Keyword.get(opts, :null)

    [
      default_expr(default, type),
      null_expr(null)
    ]
  end

  defp column_change({:add, _name, %Reference{}, _opts}) do
    raise ArgumentError, "ClickHouse does not support FOREIGN KEY"
  end

  defp column_change({:add, name, type, opts}) do
    [
      "ADD COLUMN ",
      @conn.quote_name(name),
      ?\s,
      column_type(type)
      | column_options(type, opts)
    ]
  end

  defp column_change({:modify, _name, %Reference{}, _opts}) do
    raise ArgumentError, "ClickHouse does not support FOREIGN KEY"
  end

  defp column_change({:modify, name, type, opts}) do
    [
      "MODIFY COLUMN ",
      @conn.quote_name(name),
      ?\s,
      column_type(type),
      modify_default(name, type, opts)
      | modify_null(name, opts)
    ]
  end

  defp column_change({:remove, name}) do
    ["DROP COLUMN " | @conn.quote_name(name)]
  end

  # TODO
  defp column_change({:remove, name, _type, _opts}) do
    column_change({:remove, name})
  end

  defp modify_null(_name, opts) do
    case Keyword.get(opts, :null) do
      nil -> []
      val -> null_expr(val)
    end
  end

  defp modify_default(name, type, opts) do
    case Keyword.fetch(opts, :default) do
      {:ok, _val} = ok ->
        [" ADD ", default_expr(ok, type), " FOR ", @conn.quote_name(name)]

      :error ->
        []
    end
  end

  defp null_expr(true), do: " NULL"
  defp null_expr(false), do: " NOT NULL"
  defp null_expr(_), do: []

  @dialyzer {:no_improper_lists, default_expr: 2}
  defp default_expr({:ok, nil}, _type) do
    " DEFAULT NULL"
  end

  defp default_expr({:ok, literal}, _type) when is_binary(literal) do
    [" DEFAULT '", @conn.escape_string(literal), ?']
  end

  defp default_expr({:ok, literal}, _type) when is_number(literal) do
    [" DEFAULT " | to_string(literal)]
  end

  defp default_expr({:ok, {:fragment, expr}}, _type) do
    [" DEFAULT " | expr]
  end

  defp default_expr({:ok, true}, _type) do
    " DEFAULT 1"
  end

  defp default_expr({:ok, false}, _type) do
    " DEFAULT 0"
  end

  defp default_expr({:ok, list}, _type) when is_list(list) do
    raise ArgumentError,
          "ClickHouse adapter does not support lists in :default, " <>
            "use fragments instead"
  end

  defp default_expr({:ok, map}, _type) when is_map(map) do
    raise ArgumentError,
          "ClickHouse adapter does not support maps in :default, " <>
            "use fragments instead"
  end

  defp default_expr(:error, _), do: []

  defp index_expr(literal) when is_binary(literal), do: literal
  defp index_expr(literal), do: @conn.quote_name(literal)

  defp constraint_expr(%Constraint{check: check, validate: true, comment: nil} = constraint)
       when is_binary(check) do
    [
      "CONSTRAINT ",
      @conn.quote_name(constraint.name),
      " CHECK (",
      check,
      ")"
    ]
  end

  defp constraint_expr(%Constraint{check: check, validate: true, comment: comment})
       when is_binary(check) and is_binary(comment) do
    raise "Clickhouse adapter does not support comments on check constraints"
  end

  defp constraint_expr(%Constraint{check: check, validate: false}) when is_binary(check) do
    raise "Clickhouse adapter does not support check constraints without validation on creation"
  end

  defp constraint_expr(%Constraint{exclude: exclude}) when is_binary(exclude) do
    raise "Clickhouse adapter does not support exclude constraints"
  end

  defp options_expr(nil), do: []

  defp options_expr(options) when is_list(options) do
    raise ArgumentError, "ClickHouse adapter does not support lists in :options"
  end

  defp options_expr(options), do: [?\s | to_string(options)]

  defp column_type(type) when type in [:serial, :bigserial] do
    raise ArgumentError,
          "type #{inspect(type)} is not supported as ClickHouse does not support AUTOINCREMENT"
  end

  defp column_type(:id) do
    raise ArgumentError, "type :id is ambiguous, use a literal (e.g. :Int64 or :UInt64) instead"
  end

  defp column_type(:numeric) do
    raise ArgumentError, "type :numeric is not supported"
  end

  defp column_type(:time) do
    raise ArgumentError, "type :time is not supported"
  end

  defp column_type(:map) do
    raise ArgumentError,
          ~s[type :map is ambiguous, use a literal (e.g. :JSON or :"Map(String, UInt8)") instead]
  end

  defp column_type(:decimal) do
    raise ArgumentError,
          ~s[type :decimal is ambiguous, use a literal (e.g. :"Decimal(p, s)") instead]
  end

  defp column_type(:uuid), do: "UUID"
  defp column_type(:boolean), do: "Bool"
  defp column_type(:integer), do: "Int32"
  defp column_type(:bigint), do: "Int64"

  defp column_type(type) when type in [:string, :binary, :binary_id] do
    "String"
  end

  defp column_type(:float), do: "Float64"

  defp column_type({:array, type}) do
    ["Array(", column_type(type), ?)]
  end

  # TODO DateTime('UTC')
  defp column_type(:utc_datetime), do: "DateTime"
  defp column_type(:utc_datetime_usec), do: "DateTime64(6)"
  defp column_type(:naive_datetime), do: "DateTime"
  defp column_type(:naive_datetime_usec), do: "DateTime64(6)"
  defp column_type(type), do: Atom.to_string(type)
end
