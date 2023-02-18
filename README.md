Ecto Adapter for ClickHouse using [`:ch`](https://github.com/ruslandoga/ch)

```elixir
iex> Mix.install([{:chto, github: "ruslandoga/chto"}])

iex> defmodule Repo do
       use Ecto.Repo, adapter: Ecto.Adapters.ClickHouse, otp_app: :example
     end

iex> import Ecto.Query
iex> Repo.start_link()

iex> {:ok, _} = Repo.query("CREATE TABLE default.example(a UInt32, b String, c DateTime) engine=Memory")

iex> defmodule Example do
       use Ecto.Schema

       @primary_key false
       schema "example" do
         field :a, Ch.Types.UInt32
         field :b, :string
         field :c, :naive_datetime
       end
     end

iex> Repo.insert_all(Example, [%{a: 5, b: "5"}, %{a: 6}])
{2, nil}

iex> Repo.insert_all(Example, select(Example, [e], %{a: e.a, b: e.b}))
{2, nil}

iex> Example |> limit(2) |> Repo.all()
[
  %Example{
    a: 5,
    b: "5",
    c: ~N[1970-01-01 00:00:00]
  },
  %Example{
    a: 6,
    b: "",
    c: ~N[1970-01-01 00:00:00]
  }
]

iex> Repo.update_all(Example, set: [a: 2])
# ** (Ecto.QueryError) ClickHouse does not support UPDATE statements -- use ALTER TABLE instead in query:
# from e0 in Dev.Example,
#   update: [set: [a: ^...]]

iex> {_, _} = Repo.delete_all(Example, settings: [allow_experimental_lightweight_delete: 1])
iex> Repo.aggregate(Example, :count)
0

iex> Repo.query!("DROP TABLE default.example")
```
