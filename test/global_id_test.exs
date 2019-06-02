defmodule GlobalIdTest do
  use ExUnit.Case

  setup_all do
    # Not really a fan of mocking, but it allows to play with time in tests.
    start_timestamp_mock()
    :ok
  end

  setup do
    # Each test starts at timestamp 1000.
    set_time(1000)
    GlobalId.reset()
    :ok
  end

  test "two subsequent calls during the same millisecond return different IDs" do
    {:ok, first_id} = get_id()
    {:ok, second_id} = get_id()
    assert first_id != second_id
  end

  test "calls at different timestamps return different IDs" do
    {:ok, first_id} = get_id()

    move_time(1)
    {:ok, second_id} = get_id()

    assert first_id != second_id
  end

  test "ID is not returned if the clock drifts in the past" do
    move_time(-1)

    assert :timeout = get_id()
  end

  test "unique ID is returned when clock is back on track after drifting in the past" do
    {:ok, first_id} = get_id()
    move_time(-1)
    :timeout = get_id()

    move_time(1)
    {:ok, second_id} = get_id()
    assert first_id != second_id
  end

  test "a call after a node crash returns a unique ID if the timestamp changes" do
    {:ok, first_id} = get_id()

    GlobalId.reset()
    move_time(1)

    {:ok, second_id} = get_id()
    assert first_id != second_id
  end

  # Test for a known failure scenario. Persistent storage would fix the issue. Writing to persistent
  # storage and reading it on startup would fix the issue.
  test "a call after a node crash returns a non-unique ID if the timestamp doesn't change" do
    {:ok, first_id} = get_id()

    GlobalId.reset()

    {:ok, second_id} = get_id()
    assert first_id == second_id
  end

  # Test for a known failure scenario. Writing the last used timestamp to persistent storage and
  # reading it on startup would fix the issue.
  test "a call after a node crash returns a non-unique ID if the clock drifts in the past" do
    move_time(1)
    {:ok, first_id} = get_id()

    move_time(-1)
    GlobalId.reset()
    move_time(1)

    {:ok, second_id} = get_id()
    assert first_id == second_id
  end

  test "at most 4096 unique IDs can be generated during one millisecond" do
    move_time(1)

    for _ <- 1..4096 do
      assert {:ok, _} = get_id()
    end

    assert :timeout = get_id()
    move_time(1)
    assert {:ok, _} = get_id()
  end

  defp start_timestamp_mock() do
    start_supervised!(%{
      id: __MODULE__,
      start: {Agent, :start_link, [fn -> 0 end, [name: __MODULE__]]}
    })

    :meck.new(GlobalId, [:passthrough])

    :meck.expect(GlobalId, :timestamp, fn ->
      Agent.get(__MODULE__, & &1)
    end)
  end

  defp set_time(ts) do
    Agent.update(__MODULE__, fn _ -> ts end)
  end

  defp move_time(by) do
    Agent.update(__MODULE__, fn ts -> ts + by end)
  end

  defp get_id() do
    task = Task.async(fn -> GlobalId.get_id() end)

    case Task.yield(task, 1000) do
      nil ->
        Task.shutdown(task)
        :timeout

      {:ok, _} = result ->
        result
    end
  end
end
