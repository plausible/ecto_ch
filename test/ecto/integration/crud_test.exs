defmodule Ecto.Integration.CrudTest do
  use Ecto.Integration.Case

  alias Ecto.Integration.TestRepo
  alias EctoClickHouse.Integration.{Account, AccountUser, Product, User}

  import Ecto.Query

  describe "insert" do
    test "insert user" do
      {:ok, user1} = TestRepo.insert(%User{id: 1, name: "John"}, [])
      assert user1

      {:ok, user2} = TestRepo.insert(%User{id: 2, name: "James"}, [])
      assert user2

      assert user1.id != user2.id

      user =
        User
        |> select([u], u)
        |> where([u], u.id == ^user1.id)
        |> TestRepo.one()

      assert user.name == "John"
    end

    test "handles nulls when querying correctly" do
      {:ok, account} =
        %Account{id: 1, name: "Something"}
        |> TestRepo.insert()

      {:ok, product} =
        %Product{
          id: 1,
          name: "Thing",
          account_id: account.id,
          approved_at: nil
        }
        |> TestRepo.insert()

      found = TestRepo.get(Product, product.id)
      assert found.id == product.id
      assert found.approved_at == ~N[1970-01-01 00:00:00]
      assert found.description == ""
      assert found.name == "Thing"
      assert found.tags == []
    end

    test "insert_all" do
      timestamp = NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:second)

      account = %{
        name: "John",
        inserted_at: timestamp,
        updated_at: timestamp
      }

      {1, nil} = TestRepo.insert_all(Account, [account], [])
    end
  end

  describe "delete" do
    @delete_opts [settings: [allow_experimental_lightweight_delete: 1, mutations_sync: 1]]

    test "deletes user" do
      {:ok, user} = TestRepo.insert(%User{id: 1, name: "John"}, [])
      {:ok, _} = TestRepo.delete(user, @delete_opts)
      assert TestRepo.aggregate(User, :count) == 0
    end

    test "delete_all deletes one product" do
      TestRepo.insert!(%Product{name: "hello"})
      assert {0, _} = TestRepo.delete_all(Product, @delete_opts)
      assert TestRepo.aggregate(Product, :count) == 0
    end

    test "delete_all deletes all products" do
      TestRepo.insert!(%Product{name: "hello"})
      TestRepo.insert!(%Product{name: "hello again"})
      assert {0, _} = TestRepo.delete_all(Product, @delete_opts)
      assert TestRepo.aggregate(Product, :count) == 0
    end

    test "delete_all deletes selected products" do
      TestRepo.insert!(%Product{name: "hello"})
      TestRepo.insert!(%Product{name: "hello again"})
      assert {0, nil} = TestRepo.delete_all(where(Product, name: "hello"), @delete_opts)
      assert TestRepo.aggregate(Product, :count) == 1
    end
  end

  describe "update" do
    test "updates user" do
      {:ok, user} = TestRepo.insert(%User{id: 1, name: "John"}, [])
      changeset = User.changeset(user, %{name: "Bob"})

      assert_raise ArgumentError, ~r/ClickHouse does not support UPDATE statements/, fn ->
        TestRepo.update(changeset)
      end
    end

    test "update_all returns correct rows format" do
      assert_raise Ecto.QueryError, ~r/ClickHouse does not support UPDATE statements/, fn ->
        # update with no return value should have nil rows
        TestRepo.update_all(User, set: [name: "WOW"])
      end

      {:ok, _lj} = TestRepo.insert(%User{name: "Lebron James"}, [])

      # update with returning that updates nothing should return [] rows
      no_match_query =
        from(
          u in User,
          where: u.name == "Michael Jordan",
          select: %{name: u.name}
        )

      assert_raise Ecto.QueryError, ~r/ClickHouse does not support UPDATE statements/, fn ->
        TestRepo.update_all(no_match_query, set: [name: "G.O.A.T"])
      end

      # update with returning that updates something should return resulting RETURNING clause correctly
      match_query =
        from(
          u in User,
          where: u.name == "Lebron James",
          select: %{name: u.name}
        )

      assert_raise Ecto.QueryError, ~r/ClickHouse does not support UPDATE statements/, fn ->
        TestRepo.update_all(match_query, set: [name: "G.O.A.T"])
      end
    end

    test "update_all handles null<->nil conversion correctly" do
      _account = TestRepo.insert!(%Account{name: "hello"})

      assert_raise Ecto.QueryError, ~r/ClickHouse does not support UPDATE statements/, fn ->
        TestRepo.update_all(Account, set: [name: nil])
        # assert %Account{name: nil} = TestRepo.reload(account)
      end
    end
  end

  describe "preloading" do
    test "preloads many to many relation" do
      account1 = TestRepo.insert!(%Account{id: 1, name: "Main"})
      account2 = TestRepo.insert!(%Account{id: 2, name: "Secondary"})
      user1 = TestRepo.insert!(%User{id: 1, name: "John"}, [])
      user2 = TestRepo.insert!(%User{id: 2, name: "Shelly"}, [])
      TestRepo.insert!(%AccountUser{id: 1, user_id: user1.id, account_id: account1.id})
      TestRepo.insert!(%AccountUser{id: 2, user_id: user1.id, account_id: account2.id})
      TestRepo.insert!(%AccountUser{id: 3, user_id: user2.id, account_id: account2.id})

      accounts = from(a in Account, preload: [:users]) |> TestRepo.all()

      assert Enum.count(accounts) == 2

      Enum.each(accounts, fn account ->
        assert Ecto.assoc_loaded?(account.users)
      end)
    end
  end

  describe "select" do
    test "can handle in" do
      TestRepo.insert!(%Account{id: 1, name: "hi"})
      assert [] = TestRepo.all(from(a in Account, where: a.name in ["404"]))
      assert [_] = TestRepo.all(from(a in Account, where: a.name in ["hi"]))
    end

    test "handles case sensitive text" do
      TestRepo.insert!(%Account{id: 1, name: "hi"})
      assert [_] = TestRepo.all(from(a in Account, where: a.name == "hi"))
      assert [] = TestRepo.all(from(a in Account, where: a.name == "HI"))
    end

    # TODO
    @tag skip: true
    test "handles case insensitive text" do
      TestRepo.insert!(%Account{id: 1, name: "hi", email: "hi@hi.com"})
      assert [_] = TestRepo.all(from(a in Account, where: a.email == "hi@hi.com"))
      assert [_] = TestRepo.all(from(a in Account, where: a.email == "HI@HI.COM"))
    end

    # TODO
    @tag skip: true
    test "handles exists subquery" do
      account1 = TestRepo.insert!(%Account{id: 1, name: "Main"})
      user1 = TestRepo.insert!(%User{name: "John"}, [])
      TestRepo.insert!(%AccountUser{user_id: user1.id, account_id: account1.id})

      subquery = from(au in AccountUser, where: au.user_id == parent_as(:user).id, select: 1)

      assert [_] = TestRepo.all(from(a in Account, as: :user, where: exists(subquery)))
    end

    test "can handle fragment literal" do
      account1 = TestRepo.insert!(%Account{id: 1, name: "Main"})

      name = "name"
      query = from(a in Account, where: fragment("? = ?", literal(^name), "Main"))

      assert [account] = TestRepo.all(query)
      assert account.id == account1.id
    end

    test "can handle selected_as" do
      TestRepo.insert!(%Account{id: 1, name: "Main"})
      TestRepo.insert!(%Account{id: 2, name: "Main"})
      TestRepo.insert!(%Account{id: 3, name: "Main2"})
      TestRepo.insert!(%Account{id: 4, name: "Main3"})

      query =
        from(a in Account,
          select: %{
            name: selected_as(a.name, :name2),
            count: count()
          },
          group_by: selected_as(:name2),
          order_by: selected_as(:name2)
        )

      assert [
               %{name: "Main", count: 2},
               %{name: "Main2", count: 1},
               %{name: "Main3", count: 1}
             ] = TestRepo.all(query)
    end

    test "can handle floats" do
      TestRepo.insert!(%Account{id: 1, name: "Main"})

      one = 1.0
      two = 2.0

      query =
        from(a in Account,
          select: %{
            sum: ^one + ^two
          }
        )

      assert [%{sum: 3.0}] = TestRepo.all(query)
    end
  end
end
