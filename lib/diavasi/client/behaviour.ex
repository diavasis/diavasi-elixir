defmodule Diavasi.Client.Behaviour do
  @moduledoc """
  Callbacks implemented by `Diavasi.Client`.

  Install another implementation with `Diavasi.Client.put_client/1`.
  `config :diavasi_client, client: MyApp.Diavasi` is the same switch.
  `Diavasi.Client.reset_client/0` restores `Diavasi.Client`.

  With [Mox](https://hexdocs.pm/mox), define the mock before the tests call the client:

      Mox.defmock(MyApp.DiavasiMock, for: Diavasi.Client.Behaviour)
      Diavasi.Client.put_client(MyApp.DiavasiMock)

  Call `Mox.set_mox_global()` when the code under test runs in another process.

  A test double can be a normal module:

      defmodule MyApp.Diavasi do
        @behaviour Diavasi.Client.Behaviour

        @impl true
        def start_link(opts), do: {:ok, opts[:group]}

        @impl true
        def stream(_client), do: []

        @impl true
        def next_batch(_client), do: :done

        @impl true
        def ack(_client, batch_id) when is_integer(batch_id), do: :ok

        @impl true
        def leave(_client), do: :ok

        @impl true
        def disconnect(_client), do: :ok

        @impl true
        def run(opts), do: {:ok, [opts[:total]], [1]}
      end

  """

  @typedoc """
  One `c:start_link/1` option.

    * `{:addr, String.t()}` - data-plane address `host:port`. Required.
    * `{:ca, Path.t()}` - path of the data-plane CA file. Required.
    * `{:token, String.t()}` - bearer token. Required.
    * `{:group, String.t()}` - group id. Required.
    * `{:consumer, String.t()}` - consumer id. Optional. The real client uses `"elixir"`.
    * `{:max_in_flight, non_neg_integer()}` - unacked batches the client will hold.
      Optional. The real client uses `1`.
  """
  @type start_option ::
          {:addr, String.t()}
          | {:ca, Path.t()}
          | {:token, String.t()}
          | {:group, String.t()}
          | {:consumer, String.t()}
          | {:max_in_flight, non_neg_integer()}

  @typedoc """
  Keyword list for `c:start_link/1`.

  ## Examples

      iex> opts = [
      ...>   addr: "127.0.0.1:7710",
      ...>   ca: "/tmp/diavasi-sdk/dataplane-ca.crt",
      ...>   token: "sdk-demo",
      ...>   group: "demo",
      ...>   consumer: "elixir",
      ...>   max_in_flight: 1
      ...> ]
      iex> {opts[:addr], opts[:group], opts[:consumer], opts[:max_in_flight]}
      {"127.0.0.1:7710", "demo", "elixir", 1}
  """
  @type start_options :: [start_option()]

  @typedoc """
  Batch yielded by `c:next_batch/1` and `c:stream/1`.

  `batch_id` is the integer passed to `c:ack/2`. `records` is a list of
  `Diavasi.Data.V1.Record` structs (`record_id` and `payload`).

  ## Examples

      iex> batch = %Diavasi.Data.V1.RecordBatch{
      ...>   batch_id: 4,
      ...>   records: [%Diavasi.Data.V1.Record{record_id: 1, payload: "a"}]
      ...> }
      iex> {batch.batch_id, hd(batch.records).record_id, hd(batch.records).payload}
      {4, 1, "a"}
  """
  @type batch :: Diavasi.Data.V1.RecordBatch.t()

  @typedoc """
  One `c:run/1` option: any `t:start_option/0`, plus:

    * `{:total, non_neg_integer()}` - records to ack. Required by the real client.
    * `{:halt_after, non_neg_integer()}` - after this many acks, disconnect
      without Leave. Optional.
  """
  @type run_option ::
          start_option()
          | {:total, non_neg_integer()}
          | {:halt_after, non_neg_integer()}

  @typedoc """
  Keyword list for `c:run/1`.

  ## Examples

      iex> opts = [addr: "127.0.0.1:7710", group: "demo", total: 8, halt_after: 1]
      iex> {opts[:total], opts[:halt_after], opts[:group]}
      {8, 1, "demo"}
  """
  @type run_options :: [run_option()]

  @doc """
  Open a client and join the group.

  `opts` is a `t:start_options/0`. The real client requires `:addr`, `:ca`,
  `:token`, and `:group`.

  Returns `{:ok, pid}` after Hello, JoinGroup, and FlowControl succeed.
  Returns `{:error, reason}` when connect or the handshake fails. `reason` is
  the transport error, such as `:econnrefused`, a string
  `"protocol error N: message"` or `"grpc STATUS: message"`, or an init
  failure: `{%KeyError{}, stacktrace}`, `{{:badmatch, parts}, stacktrace}`,
  or `{:badarg, stacktrace}`.

  ## Examples

      def start_link(opts), do: {:ok, opts[:group]}
  """
  @callback start_link(start_options()) :: GenServer.on_start()

  @doc """
  Yield `t:batch/0` values from `client`.

  The real client returns a stream. It stops after `c:next_batch/1` returns
  `:done`. It raises `RuntimeError` when `c:next_batch/1` returns
  `{:error, reason}`, and the message is `reason`.

  ## Examples

      def stream(_client), do: []
  """
  @callback stream(client :: term()) :: Enumerable.t()

  @doc """
  Take the next batch from `client`.

  Returns `{:ok, batch}` with a `t:batch/0`, `:done` when the stream is
  finished, or `{:error, reason}` with a string. The real client exits with
  `:timeout` if the server sends nothing for 60 seconds, and exits with
  `:noproc` if `client` is not alive.

  ## Examples

      def next_batch(_client), do: :done
  """
  @callback next_batch(client :: term()) :: {:ok, batch()} | :done | {:error, String.t()}

  @doc """
  Ack `batch_id` on `client`.

  Returns `:ok`. The real client sends an Ack frame and does not wait for a
  reply. `GenServer.call/3` exits with `:noproc` if `client` is down, and with
  `:timeout` after 5 seconds.

  ## Examples

      def ack(_client, batch_id) when is_integer(batch_id), do: :ok
  """
  @callback ack(client :: term(), batch_id :: non_neg_integer()) :: :ok

  @doc """
  Send Leave and half-close the request stream.

  Returns `:ok`. The real client exits with `:noproc` if `client` is down, and
  with `:timeout` after 5 seconds.

  ## Examples

      def leave(_client), do: :ok
  """
  @callback leave(client :: term()) :: :ok

  @doc """
  Drop `client` without sending Leave.

  Returns `:ok`. Unacked batches stay with the server for replay. The real
  client stops the process when it is alive, and returns `:ok` when it is
  already gone.

  ## Examples

      def disconnect(_client), do: :ok
  """
  @callback disconnect(client :: term()) :: :ok

  @doc """
  Ack `opts[:total]` records, then Leave.

  `opts` is a `t:run_options/0`. When `opts[:halt_after]` is an integer, the
  real client disconnects after that many acks and does not send Leave.

  Returns `{:ok, record_ids, batch_ids}`. Both lists are non-negative integers,
  in ack order. Returns `{:error, reason}`:

    * `"protocol error N: message"`
    * `"grpc STATUS: message"`
    * `"incomplete consume records=N batches=M"` when the stream ends early
    * `inspect(reason)` for a transport error, so `:econnrefused` becomes `":econnrefused"`

  Raises `KeyError` when `:total` is missing. A bad address is the
  `c:start_link/1` error tuple.

  ## Examples

      def run(opts), do: {:ok, [opts[:total]], [1]}
  """
  @callback run(run_options()) ::
              {:ok, [non_neg_integer()], [non_neg_integer()]} | {:error, String.t()}
end
