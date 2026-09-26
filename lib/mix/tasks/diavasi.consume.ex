defmodule Mix.Tasks.Diavasi.Consume do
  use Mix.Task

  @shortdoc "Consume a group over the TLS gRPC data plane"

  @moduledoc """
  Consume a Diavasi group from the command line.

      mix diavasi.consume --addr 127.0.0.1:7710 --ca /tmp/diavasi-sdk/dataplane-ca.crt \\
        --token sdk-demo --group demo --total 8
  """

  alias Diavasi.Data.Client

  def run(args) do
    Mix.Task.run("app.start")

    {opts, _} =
      OptionParser.parse!(args,
        strict: [
          addr: :string,
          ca: :string,
          token: :string,
          group: :string,
          consumer: :string,
          total: :integer,
          max_in_flight: :integer,
          halt_after: :integer
        ]
      )

    case Client.run(opts) do
      {:ok, record_ids, batch_ids} ->
        IO.puts("record_ids " <> Enum.join(record_ids, " "))
        IO.puts("batch_ids " <> Enum.join(batch_ids, " "))
        IO.puts("elixir consumed #{length(record_ids)} records in #{length(batch_ids)} batches")

      {:error, reason} ->
        Mix.shell().error(reason)

        code =
          case Regex.run(~r/protocol error (\d+)/, reason) do
            [_, n] -> String.to_integer(n)
            _ -> 1
          end

        exit({:shutdown, code})
    end
  end
end
