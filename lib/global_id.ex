defmodule GlobalId do
  @moduledoc """
  GlobalId module contains an implementation of a guaranteed globally unique id system.
  """

  import Bitwise

  @table __MODULE__

  @doc """
  Returns a globally unique, 64 bit integer.

  The ID structure is as follows

    <<timestamp::42, node_id::10, counter::12>>>

  * 42 bits for millisecond-precision timestamp (this number of bits is enough to generate IDs until
    2109-05-15T07:35:11.000000)
  * 10 bits for node ID (1024 values)
  * 12 bits left for a node-local,  monotonic counter. 2^12 = 4096

  This gives us 4096 unique values per millisecond on a single node, which is 4,096,000 values per
  second per node. If we could that many actual requests per second per node, that would mean that
  the system meets the load requirements even if only a single node is left operational. But
  unfortunately we can't.

  If we consider all the nodes, the system is able to generate 4,194,304,000 unique IDs per second.

  # TODO: explain fully qualified calls
  """
  @spec get_id() :: non_neg_integer
  def get_id() do
    last_timestamp = get_last_timestamp()
    timestamp = monotonic_timestamp(last_timestamp)
    {timestamp, counter} = get_new_timestamp_and_counter(timestamp, last_timestamp)
    set_last_timestamp(timestamp)
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
    # Even with a single thread, the benefit is that the process doesn't neet to be descheduled
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

  # This function ensures that the timestamp we use for generating IDs is always monotonically
  # increasing. If the clock used to generate timestamps drifts in the past, we wait until the new
  # timestamp is at least equal to the last used timestamp.
  # The only thing that would violate monotonicity is a clock drifting in the past after node crash
  # and restart. In that case, the node could used the timestamp it has already used in the past,
  # which could violate the global uniqueness of IDs. We could circumvent this problem by writing
  # the last used timestamp to a persistent storage every time it changes. However, that would
  # greatly reduce throughput of the system.
  @spec monotonic_timestamp(non_neg_integer) :: non_neg_integer()
  defp monotonic_timestamp(last_timestamp) do
    ts = __MODULE__.timestamp()

    if ts < last_timestamp do
      monotonic_timestamp(last_timestamp)
    else
      ts
    end
  end

  @spec next_timestamp(curr_ts :: non_neg_integer()) :: future_ts :: non_neg_integer()
  defp next_timestamp(current_timestamp) do
    ts = __MODULE__.timestamp()

    if ts <= current_timestamp do
      next_timestamp(current_timestamp)
    else
      ts
    end
  end

  @spec get_new_timestamp_and_counter(ts :: non_neg_integer(), last_ts :: non_neg_integer()) ::
          {new_ts :: non_neg_integer(), new_counter :: non_neg_integer()}
  defp get_new_timestamp_and_counter(timestamp, last_timestamp) do
    # The timestamp has changed since the last request. We can start counting from 0 again.
    if timestamp > last_timestamp do
      {timestamp, 0}
    else
      # Since the timestamp is monotonic, this branch is executed when it's equal to the last timestamp.
      # We're in the same millisecond as the previous request.
      # Keep only lower 12 bits of counter, wrapping at 4095.
      counter = band(get_counter() + 1, 4095)

      if counter == 0 do
        # If counter wrapped, it means that we used all unique IDs in current millisecond. Wait
        # until the clock moves forward in time.
        {next_timestamp(timestamp), 0}
      else
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
