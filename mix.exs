defmodule Diavasi.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/diavasis/diavasi-elixir"

  def project do
    [
      app: :diavasi,
      version: @version,
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      name: "Diavasi",
      description: "Thin client for the Diavasi data plane",
      source_url: @source_url,
      homepage_url: @source_url,
      package: package(),
      docs: docs(),
      deps: deps(),
      aliases: aliases(),
      elixirc_paths: elixirc_paths(Mix.env()),
      test_coverage: [tool: ExCoveralls],
      dialyzer: [
        plt_file: {:no_warn, "priv/plts/dialyzer.plt"},
        plt_add_apps: [:mix, :ex_unit]
      ]
    ]
  end

  def cli do
    [
      preferred_envs: [
        credo: :test,
        dialyzer: :test,
        check: :test,
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.html": :test
      ]
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
      files:
        ~w(lib/diavasi_data lib/mix/tasks/diavasi.consume.ex .formatter.exs mix.exs README.md CHANGELOG.md LICENSE proto),
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => @source_url,
        "Changelog" => "#{@source_url}/blob/v#{@version}/CHANGELOG.md"
      }
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md", "CHANGELOG.md"],
      source_url: @source_url,
      source_ref: "v#{@version}"
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp aliases do
    [
      check: ["format --check-formatted", "credo --strict", "coveralls"]
    ]
  end

  defp deps do
    [
      {:protobuf, "~> 0.14"},
      {:mint, "~> 1.5"},
      {:jason, "~> 1.4"},
      {:flow, "~> 1.2"},
      {:gen_stage, "~> 1.2"},
      {:broadway, "~> 1.0"},
      {:ex_doc, "~> 0.38", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:excoveralls, "~> 0.18", only: :test}
    ]
  end
end
