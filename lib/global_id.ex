defmodule GlobalId do
  @moduledoc """
  GlobalId module contains an implementation of a guaranteed globally unique id system.
  """

  import Bitwise

  @table __MODULE__

  @doc """
  Returns a globally unique, 64 bit integer.

  ## ID structure

  The ID structure is as follows

    <<timestamp::42, node_id::10, counter::12>>

  * 42 bits for millisecond-precision timestamp (this number of bits is enough to generate IDs until
    2109-05-15T07:35:11.000000)
  * 10 bits for node ID (1024 values)
  * 12 bits left for a node-local,  monotonic counter. 2^12 = 4096

  Including the node ID as one of the components of the global ID ensures that each worker node
  generates its own, distinct set of identifiers. Inclusion of timestamp allows us to generate a
  unique set of IDs each millisecond. The counter is used to make sure that IDs generated using the
  same timestamps are indeed different. I recommend to read the code comments now, starting from
  `get_id/0` and follow along the code.

  ## Discussion

  As noted above, the node ID allows us to generate a unique set of IDs per node, the timestamp -
  a unique set each millisecond, and the counter is a differentiator for IDs generated during the
  same millisecond. The timestamp used for generating IDs is constantly racing with the system clock.
  If the system clock drifts in the future, the timestamp will catch up. If the clock drifts in the
  past, the timestamp will "wait".

  Interesting thing is that we could get rid off the timestamp at all and use a simple logical clock
  (basically a bigger counter) if not the possibility of the node crashes. When the node starts up,
  it fetches the last timestamp from the system clock. Assuming that the clock doesn't drift in the
  past too much, the IDs we generate after the node crash are still unique. However, if the clock
  is unreliable, we might generate a couple of the same IDs (this scenario is covered by test
  cases in `test/global_id_test.exs`). If we were to solve this problem completely, we would need to
  have really well synchronized clocks (a.k.a. the Google Spanner solution), or persits the last
  timestamp used by the worker node somewhere and retrieve it from that storage after a node crash.
  However, the solution implemented here requires no coordination at all (at least from the
  worker node's code perspective, the clocks between the machines are still synchronized somehow)
  which allows for better performance.

  With given ID structure, we're theoretically able to generate 4096 unique IDs per millisecond per
  node. This gives us 4,096,000 unique values per node per second. However, that's only theory, and
  doesn't include the cost of actually running code. I've included a benchmark in `bench.exs` file
  which calls the `get_id/0` function repeatedly for a minute (you can run it with
  `MIX_ENV=prod mix bench`). On my machine, it achieved a throughput of ~300k requests per second.
  With 1024 nodes, this gives us 307,200,000 requests per second for the whole system.
  In a real world scenario, we would also need to include the cost of handling a request from the
  client.

  In `test/global_id_test.exs` you can find a suite of tests. They verify scenarios like uniqueness
  of IDs generated during the same millisecond, maximum number of unique IDs we're able to generate
  per millisecond etc. There are also tests for scenarios were the system would fail, i.e. when
  the clock drifts in the past during the node crash. However, from the exercise description
  I inferred that the clock works correctly. These tests are there just to show in which cases the
  system might fail.

  ### How do you manage uniqueness after a node crashes and restarts?

  If the system clock works correctly (i.e. is monotonic), the system preserves uniqueness by
  using the current timestamp returned by the system clock as the last timestamp used for generating
  IDs. To be more specific, it the system issued last ID at timestamp n, and restarts at
  timestamp m > n, uniqueness will be preserved.

  ### How do you manage uniqueness after the entire system fails and restarts?

  Since there is no coordination between the nodes, failure of the whole system is roughly equivalent
  to the failure of all nodes individually. Since each worker node generates a distinct set od IDs,
  and we preserve uniqueness after the node crashes (see paragraph above), then after all the nodes
  restart and crash, the uniqueness of IDs won't be violated (again, assuming the the system clocks
  are roughly monotonic).

  ### How do you handle software defects?

  When it comes to software bugs, I've written a test suite first to assert correctness of the
  solution. From the operational perspective, the worker node should be monitored and proper alerts
  should be set up on these metrics, to ensure that it works as expected. For example, we could
  monitor that it achieves desired throughput. Or we could monitor how often the system clock
  drifts in the past, to better understand the its characteristics and how the whole system is
  affected by it.
  """
  @spec get_id() :: non_neg_integer
  def get_id() do
    # Whenever a request comes in, fetch the last timestamp. This is a timestamp of when the worker
    # node started if no IDs were generated by current incarnation of the worker node yet, or the
    # timestamp used to generate the last ID.
    last_timestamp = get_last_timestamp()
    # Generate the current timestamp. We want this timestamp to be monotonic, i.e. at least equal
    # to the last timestamp used. Since the system clock might drift in the past, we wait until it
    # aligns with the last timestamp used by the node. If we didn't wait, it might happen that we
    # use the timestamp for which all the IDs have already been generated in the past.
    # Note: in a real-world scenario we might want to set an alarm if it takes too long for the
    # clock to align.
    timestamp = monotonic_timestamp(last_timestamp)
    # See the implementation of the `get_new_timestamp_and_counter/2` for further explanation.
    {timestamp, counter} = get_new_timestamp_and_counter(timestamp, last_timestamp)

    # Save in memory the timestamp used to generate a new ID as the last timestamp. It's going to be
    # used by next calls to `get_id/0`.
    set_last_timestamp(timestamp)
    # Do the same thing with counter.
    set_counter(counter)

    # timestamp << 22 | node_id() << 12 | counter
    bor(
      bsl(timestamp, 22),
      bor(bsl(node_id(), 12), counter)
    )
  end

  @doc """
  This function needs to be called before we start to accept any requests.
  """
  def reset() do
    # Since the counter and the last timestamp are both global when it comes to Erlang VM processes,
    # we need to store them somewhere where all the processes can access them.
    # We could keep them in a dedicated process, but ETS has better performance characteristics.
    # Even with a single thread, the benefit is that the process doesn't need to be descheduled
    # to fetch this global state.
    create_table()
    set_counter(0)
    set_last_timestamp(__MODULE__.timestamp())
  end

  defp create_table() do
    :ets.new(@table, [:set, :public, :named_table])
    :ok
  rescue
    ArgumentError ->
      :ok
  end

  defp set_counter(c) do
    :ets.insert(@table, {:counter, c})
  end

  defp set_last_timestamp(ts) do
    :ets.insert(@table, {:last_timestamp, ts})
  end

  @spec get_last_timestamp() :: non_neg_integer()
  defp get_last_timestamp() do
    [{:last_timestamp, ts}] = :ets.lookup(@table, :last_timestamp)
    ts
  end

  @spec get_counter() :: non_neg_integer()
  defp get_counter() do
    [{:counter, c}] = :ets.lookup(@table, :counter)
    c
  end

  # This function waits until the current timestamp is at least equal to the last used timestamp,
  # and then returns it.
  @spec monotonic_timestamp(non_neg_integer) :: non_neg_integer()
  defp monotonic_timestamp(last_timestamp) do
    ts = __MODULE__.timestamp()

    if ts < last_timestamp do
      monotonic_timestamp(last_timestamp)
    else
      ts
    end
  end

  # This function waits until the current timestamp is bigger than the provided timestamp,
  # and then returns it.
  @spec next_timestamp(ts :: non_neg_integer()) :: future_ts :: non_neg_integer()
  defp next_timestamp(timestamp) do
    ts = __MODULE__.timestamp()

    if ts <= timestamp do
      next_timestamp(timestamp)
    else
      ts
    end
  end

  @spec get_new_timestamp_and_counter(ts :: non_neg_integer(), last_ts :: non_neg_integer()) ::
          {new_ts :: non_neg_integer(), new_counter :: non_neg_integer()}
  defp get_new_timestamp_and_counter(timestamp, last_timestamp) do
    # If the current timestamp is greater than the timestamp used to generated the last ID, we can
    # start counting from 0 again.
    if timestamp > last_timestamp do
      {timestamp, 0}
    else
      # Before calling this function we made sure that `timestamp` is at least equal to the `last_timestamp`.
      # This means that this branch is only executed when they are equal.
      # Here we're generating an ID with the same timestamp as the previous request. We need to
      # increment the counter so that the new ID is different than the previous one. However, we have
      # only 12 bits for the counter, meaning that we can generate 4096 unique IDs per millisecond.
      counter = band(get_counter() + 1, 4095)

      # If the counter is 0 after incrementing, it means that it has wrapped. We can't use 0 again
      # because such ID has already been generated with the last timestamp.
      if counter == 0 do
        # We just need to wait until the current timestamp is greater than the last timestamp used
        # and start counting from 0 again.
        {next_timestamp(last_timestamp), 0}
      else
        # If the counter didn't wrap, we simply use it to generate the ID. No ID previously has
        # been generated using this combination.
        {timestamp, counter}
      end
    end
  end

  @doc """
  Returns your node id as an integer.
  It will be greater than or equal to 0 and less than or equal to 1024.
  It is guaranteed to be globally unique.
  """
  @spec node_id() :: non_neg_integer
  def node_id(), do: 1

  @doc """
  Returns timestamp since the epoch in milliseconds.
  """
  @spec timestamp() :: non_neg_integer
  def timestamp(), do: DateTime.utc_now() |> DateTime.to_unix(:millisecond)
end
