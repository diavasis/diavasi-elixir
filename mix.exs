defmodule Diavasi.MixProject do
  use Mix.Project

  def project do
    [
      app: :diavasi,
      version: "0.1.0",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      description: "Thin client for the Diavasi data plane",
      package: package(),
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger, :ssl, :public_key]
    ]
  end

  defp package do
    [
      name: "diavasi",
      files: ~w(lib/diavasi_data lib/mix/tasks/diavasi.consume.ex mix.exs README.md LICENSE proto),
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => "https://github.com/diavasis/diavasi-elixir"}
    ]
  end

  defp deps do
    [
      {:protobuf, "~> 0.14"},
      {:mint, "~> 1.5"},
      {:jason, "~> 1.4"},
      {:flow, "~> 1.2"},
      {:gen_stage, "~> 1.2"},
      {:broadway, "~> 1.0"}
    ]
  end
end
