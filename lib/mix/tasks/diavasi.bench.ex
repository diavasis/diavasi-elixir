defmodule Mix.Tasks.Diavasi.Bench do
  use Mix.Task

  @shortdoc "Run Stage 0 TCP bench client"
  @moduledoc """
  Stage 0 TCP bench. This task is not part of the Hex package.

  ## Arguments

  `args` is a list of strings. Unknown flags are ignored by `OptionParser.parse/2`.

    * `--connect` - optional string, `host:port`. Default `127.0.0.1:9800`.
    * `--group-id` - optional string. Default `bench`.
    * `--total-records` - optional integer. Default `200`.
    * `--max-in-flight` - optional integer. Default `2`.
    * `--output` - optional path. Append one JSON line.

  Returns `:ok` from `DiavasiBench.TcpClient.run/1` when the bench finishes.
  A refused connection raises `MatchError`.

  ## Examples

      iex> try do
      ...>   Mix.Tasks.Diavasi.Bench.run(~w(--connect 127.0.0.1:1 --total-records 1))
      ...> rescue
      ...>   MatchError -> :refused
      ...> end
      :refused
  """

  @spec run([String.t()]) :: :ok
  def run(args) do
    {opts, _, _} =
      OptionParser.parse(args,
        strict: [
          connect: :string,
          group_id: :string,
          total_records: :integer,
          max_in_flight: :integer,
          output: :string
        ]
      )

    DiavasiBench.TcpClient.run(opts)
  end
end
