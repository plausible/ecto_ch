defmodule Ecto.Integration.UUIDTest do
  use Ecto.Integration.Case

  alias Ecto.Integration.TestRepo
  alias EctoClickHouse.Integration.Product

  test "handles uuid serialization and deserialization with string format " do
    external_id = Ecto.UUID.generate()
    product = TestRepo.insert!(%Product{id: 1, name: "Pupper Beer", external_id: external_id})

    assert product.id
    assert product.external_id == external_id

    found = TestRepo.get(Product, product.id)
    assert found
    assert found.external_id == external_id
  end
end
