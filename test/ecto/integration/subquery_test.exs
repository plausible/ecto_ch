# Ecto SQL ships shared adapter integration tests in its Hex package.
# Rerun failures with the dependency file/line shown in the failure output;
# `mix test --failed` cannot discover imported tests through this wrapper.
ecto_sql = Map.fetch!(Mix.Project.deps_paths(), :ecto_sql)
Code.require_file("integration_test/sql/subquery.exs", ecto_sql)
