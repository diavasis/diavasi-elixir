defmodule Mix.Tasks.Diavasi.Consume do
  use Mix.Task

  @shortdoc "Consume a group over the TLS gRPC data plane"

  @moduledoc """
  Consume a Diavasi group from the command line.

      mix diavasi.consume --addr 127.0.0.1:7710 --ca /tmp/diavasi-sdk/dataplane-ca.crt \\
        --token sdk-demo --group demo --total 8

  ## Arguments

  `args` is a list of strings. Unknown flags raise `OptionParser.ParseError`.

    * `--addr` - required `String.t()`, data-plane `host:port`.
    * `--ca` - required `String.t()`, data-plane CA file.
    * `--token` - required `String.t()`, bearer token.
    * `--group` - required `String.t()`, group id.
    * `--total` - required integer, records to ack.
    * `--consumer` - optional string. `Diavasi.Client` uses `"elixir"` when omitted.
    * `--max-in-flight` - optional integer. Default `1`.
    * `--halt-after` - optional integer. After this many acks, disconnect without Leave.

  ## Returns

  On success, prints three lines and returns `:ok`:

      record_ids 1 2
      batch_ids 4
      elixir consumed 2 records in 1 batches

  On `{:error, reason}` from `Diavasi.Client.run/1`, prints `reason` to the
  shell and exits `{:shutdown, code}`. `code` is the integer in
  `"protocol error N"`, or `1` for every other error.

  ## Examples

      iex> Diavasi.Client.put_client(Diavasi.Client.Mock)
      :ok
      iex> Mox.stub(Diavasi.Client.Mock, :run, fn opts ->
      ...>   8 = opts[:total]
      ...>   "demo" = opts[:group]
      ...>   "elixir" = opts[:consumer]
      ...>   1 = opts[:max_in_flight]
      ...>   2 = opts[:halt_after]
      ...>   {:ok, [1, 2], [4]}
      ...> end)
      iex> ExUnit.CaptureIO.capture_io(fn ->
      ...>   Mix.Tasks.Diavasi.Consume.run(
      ...>     ~w(--addr 127.0.0.1:7710 --ca /tmp/diavasi-sdk/dataplane-ca.crt --token sdk-demo --group demo --consumer elixir --total 8 --max-in-flight 1 --halt-after 2)
      ...>   )
      ...> end)
      "record_ids 1 2\\nbatch_ids 4\\nelixir consumed 2 records in 1 batches\\n"
      iex> Diavasi.Client.reset_client()
      :ok

      iex> try do
      ...>   Mix.Tasks.Diavasi.Consume.run(~w(--nope))
      ...> rescue
      ...>   OptionParser.ParseError -> :parse_error
      ...> end
      :parse_error

      iex> Diavasi.Client.put_client(Diavasi.Client.Mock)
      :ok
      iex> Mox.stub(Diavasi.Client.Mock, :run, fn _opts ->
      ...>   {:error, "protocol error 5: not running"}
      ...> end)
      iex> catch_exit(
      ...>   ExUnit.CaptureIO.capture_io(:stderr, fn ->
      ...>     Mix.Tasks.Diavasi.Consume.run(
      ...>       ~w(--addr 127.0.0.1:7710 --ca ca.pem --token t --group demo --total 1)
      ...>     )
      ...>   end)
      ...> )
      {:shutdown, 5}
      iex> Diavasi.Client.reset_client()
      :ok
  """

  alias Diavasi.Client

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
