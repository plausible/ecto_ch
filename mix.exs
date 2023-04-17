defmodule Chto.MixProject do
  use Mix.Project

  @source_url "https://github.com/plausible/chto"
  @version "0.1.0"

  def project do
    [
      app: :chto,
      version: @version,
      elixir: "~> 1.14",
      elixirc_paths: elixirc_paths(Mix.env()),
      test_paths: test_paths(System.get_env("INTEGRATION")),
      deps: deps(),
      name: "Chto",
      description: "ClickHouse adapter for Ecto",
      docs: docs(),
      package: package(),
      source_url: @source_url
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(:dev), do: ["lib", "dev"]
  defp elixirc_paths(_env), do: ["lib"]

  defp test_paths(nil), do: ["test"]
  defp test_paths(_any), do: ["integration_test"]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ch, github: "plausible/ch"},
      {:ecto_sql, "~> 3.9"},
      {:benchee, "~> 1.1", only: :bench},
      {:dialyxir, "~> 1.2", only: [:dev, :test], runtime: false},
      {:ex_doc, ">= 0.0.0", only: :docs}
    ]
  end

  defp docs do
    [
      source_url: @source_url,
      source_ref: "v#{@version}",
      main: "readme",
      extras: ["README.md", "CHANGELOG.md"],
      skip_undefined_reference_warnings_on: ["CHANGELOG.md"],
      groups_for_modules: [
        "Ecto Types": [
          Ch.Types.UInt8,
          Ch.Types.UInt16,
          Ch.Types.UInt32,
          Ch.Types.UInt64,
          Ch.Types.UInt128,
          Ch.Types.UInt256,
          Ch.Types.Int8,
          Ch.Types.Int16,
          Ch.Types.Int32,
          Ch.Types.Int64,
          Ch.Types.Int128,
          Ch.Types.Int256,
          Ch.Types.Float32,
          Ch.Types.Float64,
          Ch.Types.FixedString,
          Ch.Types.Nullable,
          Ch.Types.Decimal32,
          Ch.Types.Decimal64,
          Ch.Types.Decimal128,
          Ch.Types.Decimal256
        ]
      ]
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url}
    ]
  end
end
