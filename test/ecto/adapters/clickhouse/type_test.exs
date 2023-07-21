defmodule Ecto.Adapters.ClickHouse.TypeTest do
  use ExUnit.Case, async: true

  @adapter Ecto.Adapters.ClickHouse

  # TODO
  # when is adapter_dump called?
  # Repo.insert_all: called for rows?
  # Repo.insert: called for struct?
  # Repo.all: called for params?
  # benchmark &mod:fun/_ vs others

  describe "adapter_dump" do
    test "Ecto.UUID" do
      type = Ecto.UUID
      uuid = Ecto.UUID.generate()
      assert Ecto.Type.adapter_dump(@adapter, type, uuid) == {:ok, uuid}
      assert Ecto.Type.adapter_dump(@adapter, type, Ecto.UUID.dump!(uuid)) == {:ok, uuid}
      assert Ecto.Type.adapter_dump(@adapter, {:array, type}, [uuid]) == {:ok, [uuid]}

      assert Ecto.Type.adapter_dump(@adapter, {:array, type}, [Ecto.UUID.dump!(uuid)]) ==
               {:ok, [uuid]}
    end

    test "UUID" do
      type = Ecto.ParameterizedType.init(Ch, type: "UUID")
      uuid = Ecto.UUID.generate()
      assert Ecto.Type.adapter_dump(@adapter, type, uuid) == {:ok, uuid}
      assert Ecto.Type.adapter_dump(@adapter, type, Ecto.UUID.dump!(uuid)) == {:ok, uuid}
      assert Ecto.Type.adapter_dump(@adapter, {:array, type}, [uuid]) == {:ok, [uuid]}

      assert Ecto.Type.adapter_dump(@adapter, {:array, type}, [Ecto.UUID.dump!(uuid)]) ==
               {:ok, [uuid]}
    end

    test "Nullable(UUID)" do
      type = Ecto.ParameterizedType.init(Ch, type: "Nullable(UUID)")
      uuid = Ecto.UUID.generate()
      assert Ecto.Type.adapter_dump(@adapter, type, uuid) == {:ok, uuid}
      assert Ecto.Type.adapter_dump(@adapter, type, Ecto.UUID.dump!(uuid)) == {:ok, uuid}
      assert Ecto.Type.adapter_dump(@adapter, {:array, type}, [uuid]) == {:ok, [uuid]}

      assert Ecto.Type.adapter_dump(@adapter, {:array, type}, [Ecto.UUID.dump!(uuid)]) ==
               {:ok, [uuid]}
    end

    test "String" do
      type = Ecto.ParameterizedType.init(Ch, type: "String")
      utf8 = "hello"
      not_utf8 = "\x61\xF0\x80\x80\x80b"
      assert Ecto.Type.adapter_dump(@adapter, type, utf8) == {:ok, utf8}
      assert Ecto.Type.adapter_dump(@adapter, type, not_utf8) == {:ok, not_utf8}

      assert Ecto.Type.adapter_dump(@adapter, {:array, type}, [utf8, not_utf8]) ==
               {:ok, [utf8, not_utf8]}
    end

    test "Bool" do
      type = Ecto.ParameterizedType.init(Ch, type: "Bool")
      assert Ecto.Type.adapter_dump(@adapter, type, true) == {:ok, true}
      assert Ecto.Type.adapter_dump(@adapter, type, false) == {:ok, false}

      assert Ecto.Type.adapter_dump(@adapter, {:array, type}, [true, false]) ==
               {:ok, [true, false]}
    end

    for size <- [8, 16, 32, 64, 128, 256] do
      test "Int#{size}" do
        type = Ecto.ParameterizedType.init(Ch, type: "Int#{unquote(size)}")
        assert Ecto.Type.adapter_dump(@adapter, type, 0) == {:ok, 0}
        assert Ecto.Type.adapter_dump(@adapter, type, 1) == {:ok, 1}
        assert Ecto.Type.adapter_dump(@adapter, type, -1) == {:ok, -1}
        rand = :rand.uniform(1_000_000) - 500_000
        assert Ecto.Type.adapter_dump(@adapter, type, rand) == {:ok, rand}

        assert Ecto.Type.adapter_dump(@adapter, {:array, type}, [0, 1, -1, rand]) ==
                 {:ok, [0, 1, -1, rand]}
      end

      test "UInt#{size}" do
        type = Ecto.ParameterizedType.init(Ch, type: "UInt#{unquote(size)}")
        assert Ecto.Type.adapter_dump(@adapter, type, 0) == {:ok, 0}
        assert Ecto.Type.adapter_dump(@adapter, type, 1) == {:ok, 1}
        rand = :rand.uniform(1_000_000)
        assert Ecto.Type.adapter_dump(@adapter, type, rand) == {:ok, rand}

        assert Ecto.Type.adapter_dump(@adapter, {:array, type}, [0, 1, rand]) ==
                 {:ok, [0, 1, rand]}
      end
    end

    for size <- [32, 64] do
      test "Float#{size}" do
        type = Ecto.ParameterizedType.init(Ch, type: "Float#{unquote(size)}")
        assert Ecto.Type.adapter_dump(@adapter, type, -1.1) == {:ok, -1.1}
        assert Ecto.Type.adapter_dump(@adapter, type, 1.1) == {:ok, 1.1}
        assert Ecto.Type.adapter_dump(@adapter, {:array, type}, [-1.1, 1.1]) == {:ok, [-1.1, 1.1]}
      end
    end
  end

  describe "adapter_load" do
    for size <- [8, 16, 32, 64, 128, 256] do
      test "Int#{size}" do
        type = Ecto.ParameterizedType.init(Ch, type: "Int#{unquote(size)}")
        assert Ecto.Type.adapter_load(@adapter, type, 0) == {:ok, 0}
        assert Ecto.Type.adapter_load(@adapter, type, 1) == {:ok, 1}
        assert Ecto.Type.adapter_load(@adapter, type, -1) == {:ok, -1}
        rand = :rand.uniform(1_000_000) - 500_000
        assert Ecto.Type.adapter_load(@adapter, type, rand) == {:ok, rand}
      end

      test "UInt#{size}" do
        type = Ecto.ParameterizedType.init(Ch, type: "UInt#{unquote(size)}")
        assert Ecto.Type.adapter_load(@adapter, type, 0) == {:ok, 0}
        assert Ecto.Type.adapter_load(@adapter, type, 1) == {:ok, 1}
        rand = :rand.uniform(1_000_000)
        assert Ecto.Type.adapter_load(@adapter, type, rand) == {:ok, rand}
      end
    end
  end
end
