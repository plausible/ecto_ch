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

    engine = engine || "TinyLog"
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
        options,
        " ENGINE="
        | engine
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

  def execute_ddl({command, %Constraint{} = _constraint})
      when command in [:create, :create_if_not_exists] do
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
    pk_expr =
      columns
      |> Enum.filter(fn {_, _, _, opts} ->
        case Keyword.get(opts, :primary_key, false) do
          true = t -> t
          false = f -> f
        end
      end)
      |> Enum.map(fn {_, name, type, _} ->
        if type in [:serial, :bigserial] do
          raise ArgumentError,
                "ClickHouse does not support PRIMARY KEY AUTOINCREMENT, " <>
                  "consider using a type other than :serial or :bigserial"
        end

        @conn.quote_name(name)
      end)
      |> Enum.intersperse(?,)

    [",PRIMARY KEY (", pk_expr, ?)]
  end

  defp column_definition({:add, _name, %Reference{}, _opts}) do
    raise ArgumentError, "ClickHouse does not support FOREIGN KEY"
  end

  defp column_definition({:add, name, type, opts}) do
    [
      @conn.quote_name(name),
      ?\s,
      column_type(type, opts)
      | column_options(type, opts)
    ]
  end

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
      column_type(type, opts)
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
      column_type(type, opts),
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

  defp options_expr(nil), do: []

  defp options_expr(options) when is_list(options) do
    raise ArgumentError, "ClickHouse adapter does not support lists in :options"
  end

  defp options_expr(options), do: [?\s | to_string(options)]

  defp column_type(type, nil), do: column_type(type, [])

  defp column_type(type, _opts) when type in [:serial, :bigserial] do
    raise ArgumentError,
          "ClickHouse does not support AUTOINCREMENT (:serial, :bigserial)"
  end

  defp column_type(:uuid = type, opts) do
    validate_type_opts!(type, opts, [])
    "UUID"
  end

  defp column_type(:boolean = type, opts) do
    validate_type_opts!(type, opts, [])
    "Bool"
  end

  defp column_type(type, opts) when type in [:id, :bigint, :integer] do
    [size, unsigned, low_cardinality] =
      validate_type_opts!(type, opts, [:size, :unsigned, :low_cardinality])

    size = size || 64
    base = if unsigned, do: "UInt", else: "Int"
    validate_size!(size, [8, 16, 32, 64, 128, 256])
    maybe_wrap_type(low_cardinality, "LowCardinality", "#{base}#{size}")
  end

  defp column_type(type, opts) when type in [:string, :binary, :binary_id] do
    [size, low_cardinality] = validate_type_opts!(type, opts, [:size, :low_cardinality])

    type =
      if size do
        unless is_integer(size) and size > 0 do
          raise ArgumentError, "expected :size to be positive integer, got: #{inspect(size)}"
        end

        "FixedString(#{size})"
      else
        "String"
      end

    maybe_wrap_type(low_cardinality, "LowCardinality", type)
  end

  defp column_type(type, opts) when type in [:float, :numeric] do
    [size, low_cardinality] = validate_type_opts!(type, opts, [:size, :low_cardinality])
    size = size || 64
    validate_size!(size, [32, 64])
    maybe_wrap_type(low_cardinality, "LowCardinality", "Float#{size}")
  end

  defp column_type({:array, type}, opts) do
    ["Array(", column_type(type, opts), ?)]
  end

  defp column_type(:map = type, opts) do
    [k, v, json] = validate_type_opts!(type, opts, [:key, :value, :json])

    cond do
      k && v ->
        k = column_type_map_kv(k)
        v = column_type_map_kv(v)
        "Map(#{k},#{v})"

      json ->
        "JSON"

      true ->
        raise ArgumentError, """
        Ambiguous :map type declaration.

        Please provide either:

          - :key and :value options

            add :map, :map, key: :string, value: {:integer, size: 64}
            # Map(String, Int64)

          - json: true

            add :map, :map, json: true
            # JSON
        """
    end
  end

  defp column_type(:utc_datetime = type, opts) do
    [low_cardinality] = validate_type_opts!(type, opts, [:low_cardinality])
    maybe_wrap_type(low_cardinality, "LowCardinality", "DateTime('UTC')")
  end

  defp column_type(:utc_datetime_usec = type, opts) do
    [low_cardinality] = validate_type_opts!(type, opts, [:low_cardinality])
    maybe_wrap_type(low_cardinality, "LowCardinality", "DateTime64(6,'UTC')")
  end

  defp column_type(:naive_datetime = type, opts) do
    [timezone, low_cardinality] = validate_type_opts!(type, opts, [:timezone, :low_cardinality])

    type =
      if timezone do
        "DateTime('#{timezone}')"
      else
        "DateTime"
      end

    maybe_wrap_type(low_cardinality, "LowCardinality", type)
  end

  defp column_type(:naive_datetime_usec = type, opts) do
    [timezone, low_cardinality] = validate_type_opts!(type, opts, [:timezone, :low_cardinality])

    type =
      if timezone do
        "DateTime64(6,'#{timezone}')"
      else
        "DateTime64(6)"
      end

    maybe_wrap_type(low_cardinality, "LowCardinality", type)
  end

  defp column_type(:time, _opts) do
    "Time"
  end

  defp column_type(:decimal = type, opts) do
    [precision, scale, size] = validate_type_opts!(type, opts, [:precision, :scale, :size])

    cond do
      precision && scale ->
        validate_size!(precision, 1..76)
        validate_size!(scale, 0..precision)
        "Decimal(#{precision},#{scale})"

      size && scale ->
        validate_size!(size, [32, 64, 128, 256])

        precision =
          case size do
            32 -> 9
            64 -> 18
            128 -> 38
            256 -> 76
          end

        validate_size!(scale, 0..precision)
        "Decimal#{size}(#{scale})"

      true ->
        "either :precision and :scale or :size and :scale are required for :decimal"
    end
  end

  defp column_type(type, opts) do
    validate_type_opts!(type, opts, [])
    Atom.to_string(type)
  end

  defp validate_type_opts!(type, opts, keys) do
    case Keyword.split(opts, [:default, :primary_key, :null] ++ keys) do
      {opts, []} ->
        Enum.map(keys, fn key -> opts[key] end)

      {_opts, rest} ->
        raise ArgumentError, "unsupported options for type #{inspect(type)}: #{inspect(rest)}"
    end
  end

  defp maybe_wrap_type(wrap?, wrapper, type) do
    if wrap? do
      [wrapper, ?(, type, ?)]
    else
      type
    end
  end

  defp column_type_map_kv({type, opts}), do: column_type(type, opts)
  defp column_type_map_kv(type) when is_atom(type), do: column_type(type, [])
  defp column_type_map_kv(type) when is_binary(type), do: type

  defp validate_size!(nil, valid_sizes) do
    unless nil in valid_sizes do
      raise ArgumentError, "expected :size to be positive integer, got nil"
    end
  end

  defp validate_size!(size, valid_sizes) do
    unless is_integer(size) and size > 0 do
      raise ArgumentError, "expected :size to be positive integer, got: #{inspect(size)}"
    end

    unless size in valid_sizes do
      raise ArgumentError,
            "expected :size to be in #{inspect(valid_sizes)}, got: #{size}"
    end
  end
end
