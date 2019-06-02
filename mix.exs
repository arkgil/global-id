defmodule GlobalId.MixProject do
  use Mix.Project

  def project do
    [
      app: :global_id,
      version: "0.1.0",
      elixir: "~> 1.8",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:meck, "~> 0.8", only: :test},
      {:benchee, "~> 1.0", only: :prod}
    ]
  end

  defp aliases do
    [bench: "run bench.exs"]
  end
end
