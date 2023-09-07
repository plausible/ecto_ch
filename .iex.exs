import_if_available(Ecto.Query)
alias Dev.{Repo, Example}

Application.put_env(:dev, Dev.Repo,
  database: "dev",
  settings: [path: "./.dev"],
  show_sensitive_data_on_connection_error: true,
  pool_size: 1,
  cmd: Ch.Local.clickhouse_local_cmd()
)
