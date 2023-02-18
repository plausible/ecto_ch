defmodule Ecto.Integration.CrudTest do
  use ExUnit.Case

  alias Ecto.Integration.TestRepo
  alias EctoClickHouse.Integration.{Event, Session}

  import Ecto.Query

  setup do
    on_exit(fn ->
      TestRepo.query!("truncate table events")
      TestRepo.query!("truncate table sessions")
    end)
  end

  # TODO :async_insert (check if experimental delete works while insert is async)
  # TODO insert_stream
  # TODO insert_all with stream?
  # TODO alter_update_all
  # TODO alter_delete_all

  describe "insert" do
    test "insert event" do
      {:ok, event1} = TestRepo.insert(%Event{domain: "dummy.site"}, [])
      assert event1

      {:ok, event2} = TestRepo.insert(%Event{domain: "example.com"}, [])
      assert event2

      event =
        Event
        |> where(domain: ^event1.domain)
        |> TestRepo.one()

      assert event.domain == "dummy.site"

      # defaults
      assert event.type == ""
      assert event.session_id == 0
    end

    test "insert_all" do
      timestamp = NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:second)

      event = %{
        domain: "dummy.site",
        inserted_at: timestamp
      }

      {1, nil} = TestRepo.insert_all(Event, [event], [])
    end
  end

  describe "delete" do
    @delete_opts [settings: [allow_experimental_lightweight_delete: 1]]

    test "deletes session" do
      {:ok, session} = TestRepo.insert(%Session{id: 0}, [])
      assert {:ok, %Session{}} = TestRepo.delete(session, @delete_opts)
      # assert TestRepo.aggregate(Event, :count) == 0
    end

    test "delete_all deletes one event" do
      TestRepo.insert!(%Event{domain: "dummy.site"}, [])
      assert {1, nil} = TestRepo.delete_all(Event, @delete_opts)
      # assert TestRepo.aggregate(Event, :count) == 0
    end

    test "delete_all deletes all events" do
      TestRepo.insert!(%Event{domain: "dummy.site"}, [])
      TestRepo.insert!(%Event{domain: "example.com"}, [])
      # TODO {2, nil} but we don't seem to have any info from clickhouse
      # as to how many rows have been "deleted"
      assert {1, nil} = TestRepo.delete_all(Event, @delete_opts)
      # assert TestRepo.aggregate(Event, :count) == 0
    end

    test "delete_all deletes selected events" do
      TestRepo.insert!(%Event{domain: "dummy.site"}, [])
      TestRepo.insert!(%Event{domain: "example.com"}, [])
      assert {1, nil} = TestRepo.delete_all(where(Event, domain: "example.com"), @delete_opts)
      # assert TestRepo.aggregate(Event, :count) == 1
    end
  end

  describe "update" do
    test "not supported" do
      {:ok, session} = TestRepo.insert(%Session{id: 0, domain: "dummy.site"}, [])
      changeset = Session.changeset(session, %{domain: "example.com"})

      assert_raise ArgumentError, ~r/ClickHouse does not support UPDATE statements/, fn ->
        TestRepo.update(changeset)
      end
    end

    test "update_all not supported" do
      assert_raise Ecto.QueryError, ~r/ClickHouse does not support UPDATE statements/, fn ->
        TestRepo.update_all(Event, set: [domain: "wow.com"])
      end
    end
  end

  describe "alter_update_all" do
    @tag skip: true
    test "todo"
  end

  describe "transaction" do
    test "not really supported, but no exceptions either" do
      # we are not raising an expection since db_connection is using the same
      # transaction interface for Repo.checkout (to run multiple statements on a single connection)
      # which we __do__ want to support

      assert {:ok, _changes} =
               Ecto.Multi.new()
               |> Ecto.Multi.insert(:session, fn _ ->
                 Session.changeset(%Session{id: 0}, %{domain: "test.com"})
               end)
               |> Ecto.Multi.insert(:event, fn %{session: %{id: session_id}} ->
                 Event.changeset(%Event{session_id: session_id}, %{
                   domain: "test.com",
                   type: "view"
                 })
               end)
               |> TestRepo.transaction()
    end
  end

  describe "preloading" do
    test "preloads has_many relation" do
      session1 = TestRepo.insert!(%Session{id: 1, domain: "1.com"})
      session2 = TestRepo.insert!(%Session{id: 2, domain: "2.com"})
      TestRepo.insert!(%Event{session_id: session1.id, domain: "1.com", type: "view"})
      TestRepo.insert!(%Event{session_id: session2.id, domain: "2.com", type: "view"})

      sessions = from(s in Session, preload: [:events]) |> TestRepo.all()
      assert length(sessions) == 2

      Enum.each(sessions, fn session ->
        assert Ecto.assoc_loaded?(session.events)
      end)
    end

    @tag skip: true
    test "preloads many_to_many relation"
  end

  describe "select" do
    test "can handle in" do
      TestRepo.insert!(%Event{domain: "dummy.site"})
      assert [] = TestRepo.all(from(e in Event, where: e.domain in ["example.com"]))
      assert [_] = TestRepo.all(from(e in Event, where: e.domain in ["dummy.site"]))
    end

    test "handles case sensitive text" do
      TestRepo.insert!(%Event{domain: "dummy.site"})
      assert [_] = TestRepo.all(from(e in Event, where: e.domain == "dummy.site"))
      assert [] = TestRepo.all(from(e in Event, where: e.domain == "DUMMY.SITE"))
    end

    @tag skip: true
    test "handles exists subquery" do
      TestRepo.insert!(%Session{id: 0})
      TestRepo.insert!(%Event{session_id: 0}, [])

      subquery = from(e in Event, where: e.session_id == parent_as(:session).id, select: 1)

      assert [_] = TestRepo.all(from(s in Session, as: :session, where: exists(subquery)))
    end

    test "can handle fragment literal" do
      event1 = TestRepo.insert!(%Event{domain: "dummy.site"})

      domain = "domain"
      query = from(e in Event, where: fragment("? = ?", literal(^domain), "dummy.site"))

      assert [event] = TestRepo.all(query)
      assert event.domain == event1.domain
    end

    test "can handle selected_as" do
      TestRepo.insert!(%Event{domain: "dummy.site"})
      TestRepo.insert!(%Event{domain: "dummy.site"})
      TestRepo.insert!(%Event{domain: "dummy2.site"})
      TestRepo.insert!(%Event{domain: "dummy3.site"})

      query =
        from(e in Event,
          select: %{
            domain: selected_as(e.domain, :name2),
            count: count()
          },
          group_by: selected_as(:name2),
          order_by: selected_as(:name2)
        )

      assert [
               %{domain: "dummy.site", count: 2},
               %{domain: "dummy2.site", count: 1},
               %{domain: "dummy3.site", count: 1}
             ] = TestRepo.all(query)
    end

    test "can handle floats" do
      TestRepo.insert!(%Event{domain: "dummy.site"})

      one = 1.0
      two = 2.0

      query =
        from(a in Event,
          select: %{
            sum: ^one + ^two
          }
        )

      assert [%{sum: 3.0}] = TestRepo.all(query)
    end
  end
end
