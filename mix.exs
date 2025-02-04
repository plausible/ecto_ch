defmodule EctoCh.MixProject do
  use Mix.Project

  @source_url "https://github.com/plausible/ecto_ch"
  @version "0.6.0"

  def project do
    [
      app: :ecto_ch,
      version: @version,
      elixir: "~> 1.14",
      elixirc_paths: elixirc_paths(Mix.env()),
      test_paths: test_paths(System.get_env("INTEGRATION")),
      deps: deps(),
      name: "Ecto ClickHouse",
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
      {:ch, "~> 0.3.0"},
      {:ecto_sql, "~> 3.12"},
      {:benchee, "~> 1.1", only: :bench},
      {:dialyxir, "~> 1.2", only: [:dev, :test], runtime: false},
      {:ex_doc, ">= 0.0.0", only: :docs},
      {:rexbug, "~> 1.0", only: [:dev, :test]},
      {:tz, "~> 0.28.1", only: [:dev, :test]}
    ]
  end

  defp docs do
    [
      source_url: @source_url,
      source_ref: "v#{@version}",
      main: "readme",
      extras: ["README.md", "CHANGELOG.md"],
      skip_undefined_reference_warnings_on: ["CHANGELOG.md"]
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url}
    ]
  end
end
