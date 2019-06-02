GlobalId.reset()

Benchee.run(
  %{
    "get_id" => fn -> GlobalId.get_id() end
  },
  time: 60
)
