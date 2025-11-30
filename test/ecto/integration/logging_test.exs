defmodule Ecto.Integration.LoggingTest do
  use Ecto.Integration.Case

  alias Ecto.Integration.{TestRepo, Logging, ArrayLogging}
  alias EctoClickHouse.Integration.Account

  import ExUnit.CaptureLog
  import Ecto.Query

  setup_all do
    :ok =
      :telemetry.attach_many(
        __MODULE__,
        [[:ecto, :integration, :test_repo, :query], [:custom]],
        &__MODULE__.handle_event/4,
        :ok
      )
  end

  describe "telemetry" do
    test "dispatches event" do
      log = fn event_name, measurements, metadata ->
        assert Enum.at(event_name, -1) == :query
        assert %{result: {:ok, _res}} = metadata

        assert measurements.total_time ==
                 measurements.query_time + measurements.decode_time + measurements.queue_time

        assert measurements.idle_time
        send(self(), :logged)
      end

      Process.put(:telemetry, log)
      TestRepo.all(Account)
      assert_received :logged
    end

    test "contains source" do
      log = fn _event_name, _measurements, metadata ->
        assert %{source: "accounts"} = metadata
        send(self(), :logged)
      end

      Process.put(:telemetry, log)
      TestRepo.all(Account)
      assert_received :logged
    end

    test "dispatches event with stacktrace" do
      log = fn _event_name, _measurements, metadata ->
        assert %{stacktrace: [_ | _]} = metadata
        send(self(), :logged)
      end

      Process.put(:telemetry, log)
      TestRepo.all(Account, stacktrace: true)
      assert_received :logged
    end

    test "dispatches event with custom options" do
      log = fn event_name, _measurements, metadata ->
        assert Enum.at(event_name, -1) == :query
        assert metadata.options == [:custom_metadata]
        send(self(), :logged)
      end

      Process.put(:telemetry, log)
      TestRepo.all(Account, telemetry_options: [:custom_metadata])
      assert_received :logged
    end

    test "dispatches under another event name" do
      log = fn [:custom], measurements, metadata ->
        assert %{result: {:ok, _res}} = metadata

        assert measurements.total_time ==
                 measurements.query_time + measurements.decode_time + measurements.queue_time

        assert measurements.idle_time
        send(self(), :logged)
      end

      Process.put(:telemetry, log)
      TestRepo.all(Account, telemetry_event: [:custom])
      assert_received :logged
    end

    test "is not dispatched with no event name" do
      Process.put(:telemetry, fn _, _ -> raise "never called" end)
      TestRepo.all(Account, telemetry_event: nil)
      refute_received :logged
    end

    test "cast params" do
      uuid = Ecto.UUID.generate()
      # dumped_uuid = Ecto.UUID.dump!(uuid)

      log = fn _event_name, _measurements, metadata ->
        # assert [dumped_uuid] == metadata.params
        assert [uuid] == metadata.params
        assert [uuid] == metadata.cast_params
        send(self(), :logged)
      end

      Process.put(:telemetry, log)
      TestRepo.all(from l in Logging, where: l.uuid == ^uuid)
      assert_received :logged
    end
  end

  describe "logs" do
    @stacktrace_opts [stacktrace: true, log: :error]

    defp stacktrace_entry(line) do
      ~r/↳ anonymous fn\/0 in Ecto.Integration.LoggingTest.\"test logs includes stacktraces\"\/1, at: .*test\/ecto\/integration\/logging_test.exs:#{line - 3}/
    end

    test "when some measurements are nil" do
      assert capture_log(fn -> TestRepo.query("BEG", [], log: :error) end) =~
               "[error]"
    end

    test "includes stacktraces" do
      assert capture_log(fn ->
               TestRepo.all(Account, @stacktrace_opts)

               :ok
             end) =~ stacktrace_entry(__ENV__.line)

      assert capture_log(fn ->
               TestRepo.insert(%Account{}, @stacktrace_opts)

               :ok
             end) =~ stacktrace_entry(__ENV__.line)

      # transactions are not supported
      # assert capture_log(fn ->
      #          # Test cascading options
      #          Ecto.Multi.new()
      #          |> Ecto.Multi.insert(:post, %Account{})
      #          |> TestRepo.transaction(@stacktrace_opts)

      #          :ok
      #        end) =~ stacktrace_entry(__ENV__.line)

      # assert capture_log(fn ->
      #          # In theory we should point to the call _inside_ run
      #          # but all multi calls point to the transaction starting point.
      #          Ecto.Multi.new()
      #          |> Ecto.Multi.run(:all, fn _, _ ->
      #            {:ok, TestRepo.all(Account, @stacktrace_opts)}
      #          end)
      #          |> TestRepo.transaction()

      #          :ok
      #        end) =~ stacktrace_entry(__ENV__.line)
    end

    test "with custom log level" do
      assert capture_log(fn -> TestRepo.insert!(%Account{id: 1}, log: :error) end) =~
               "[error]"

      # We cannot assert on the result because it depends on the suite log level
      capture_log(fn ->
        TestRepo.insert!(%Account{id: 1}, log: true)
      end)

      # But this assertion is always true
      assert capture_log(fn ->
               TestRepo.insert!(%Account{id: 1}, log: false)
             end) == ""
    end

    test "with a log: true override when logging is disabled" do
      refute capture_log(fn ->
               TestRepo.insert!(%Account{id: 1}, log: true)
             end) =~ "an exception was raised logging"
    end

    test "with unspecified :log option when logging is disabled" do
      refute capture_log(fn ->
               TestRepo.insert!(%Account{id: 1})
             end) =~ "an exception was raised logging"
    end
  end

  describe "parameter logging" do
    @uuid_regex ~r/[0-9a-f]{2}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i
    @naive_datetime_regex ~r/~N\[[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\]/

    @tag :skip
    test "for insert_all with query" do
      # Source query
      int = 1
      uuid = Ecto.UUID.generate()

      source_query =
        from l in Logging,
          where: l.int == ^int and l.uuid == ^uuid,
          select: %{uuid: l.uuid, int: l.int}

      # Ensure parameters are complete and in correct order
      log =
        capture_log(fn ->
          TestRepo.insert_all(Logging, source_query, log: :info)
        end)

      param_regex = ~r/\[(?<int>.+), \"(?<uuid>.+)\"\]/
      param_logs = Regex.named_captures(param_regex, log)

      # Query parameters
      assert param_logs["int"] == Integer.to_string(int)
      assert param_logs["uuid"] == uuid
    end

    @tag skip: true
    test "for insert_all with entries"
    @tag skip: true
    test "for insert_all with entries and placeholders"
    @tag skip: true
    test "for insert_all with query with conflict query"
    @tag skip: true
    test "for insert_all with entries conflict query"
    @tag skip: true
    test "for insert_all with entries, placeholders and conflict query"

    test "for insert" do
      # Insert values
      int = 1
      uuid = Ecto.UUID.generate()

      # Ensure parameters are complete and in correct order
      log =
        capture_log(fn ->
          TestRepo.insert!(%Logging{uuid: uuid, int: int},
            log: :info
          )
        end)

      param_regex =
        ~r/\[(?<int>.+), \"(?<uuid>.+)\", (?<inserted_at>.+), (?<updated_at>.+), \"(?<bid>.+)\"\]/

      param_logs = Regex.named_captures(param_regex, log)

      # User changes
      assert param_logs["int"] == Integer.to_string(int)
      assert param_logs["uuid"] == uuid
      # Autogenerated changes
      assert param_logs["inserted_at"] =~ @naive_datetime_regex
      assert param_logs["updated_at"] =~ @naive_datetime_regex
      # Filters
      assert param_logs["bid"] =~ @uuid_regex
    end

    @tag skip: true
    test "for insert with conflict query"
    @tag skip: true
    test "for update"

    test "for delete" do
      current = TestRepo.insert!(%Logging{})

      # Ensure parameters are complete and in correct order
      log =
        capture_log(fn ->
          TestRepo.delete!(current,
            log: :info,
            settings: [allow_experimental_lightweight_delete: 1]
          )
        end)

      param_regex = ~r/\[\"(?<bid>.+)\"\]/
      param_logs = Regex.named_captures(param_regex, log)

      # Filters
      assert param_logs["bid"] == current.bid
    end

    test "for queries" do
      int = 1
      uuid = Ecto.UUID.generate()

      # all
      log =
        capture_log(fn ->
          TestRepo.all(
            from(l in Logging,
              select: type(^"1", :integer),
              where: l.int == ^int and l.uuid == ^uuid
            ),
            log: :info
          )
        end)

      param_regex = ~r/\[(?<tagged_int>.+), (?<int>.+), \"(?<uuid>.+)\"\]/
      param_logs = Regex.named_captures(param_regex, log)

      assert param_logs["tagged_int"] == Integer.to_string(int)
      assert param_logs["int"] == Integer.to_string(int)
      assert param_logs["uuid"] == uuid

      ## update_all
      # update = 2

      # log =
      #   capture_log(fn ->
      #     from(l in Logging,
      #       where: l.int == ^int and l.uuid == ^uuid,
      #       update: [set: [int: ^update]]
      #     )
      #     |> TestRepo.update_all([], log: :info)
      #   end)

      # param_regex = ~r/\[(?<update>.+), (?<int>.+), \"(?<uuid>.+)\"\]/
      # param_logs = Regex.named_captures(param_regex, log)

      # assert param_logs["update"] == Integer.to_string(update)
      # assert param_logs["int"] == Integer.to_string(int)
      # assert param_logs["uuid"] == uuid

      # delete_all
      log =
        capture_log(fn ->
          TestRepo.delete_all(from(l in Logging, where: l.int == ^int and l.uuid == ^uuid),
            log: :info,
            settings: [allow_experimental_lightweight_delete: 1]
          )
        end)

      param_regex = ~r/\[(?<int>.+), \"(?<uuid>.+)\"\]/
      param_logs = Regex.named_captures(param_regex, log)

      assert param_logs["int"] == Integer.to_string(int)
      assert param_logs["uuid"] == uuid
    end

    @tag skip: true
    test "for queries with stream"

    test "for queries with array type" do
      uuid = Ecto.UUID.generate()
      uuid2 = Ecto.UUID.generate()

      log =
        capture_log(fn ->
          TestRepo.all(
            from(a in ArrayLogging, where: a.uuids == type(^[uuid, uuid2], {:array, Ecto.UUID})),
            log: :info
          )
        end)

      param_regex = ~r/\[(?<uuids>\[.+\])\]/
      param_logs = Regex.named_captures(param_regex, log)

      assert param_logs["uuids"] == "[\"#{uuid}\", \"#{uuid2}\"]"
    end
  end

  def handle_event(event, latency, metadata, _config) do
    handler = Process.delete(:telemetry) || fn _, _, _ -> :ok end
    handler.(event, latency, metadata)
  end
end
