defmodule Diavasi.Client do
  @moduledoc """
  Supervised consumer for the `diavasi.data.v1` stream.

  Put this module under a supervisor. `start_link/1` opens TLS, sends Hello
  version 1, then JoinGroup, then one FlowControl. `stream/1` and
  `next_batch/1` yield `Diavasi.Data.V1.RecordBatch`. `ack/2` acks a batch.
  `leave/1` sends Leave and half-closes the request. `disconnect/1` drops the
  socket without Leave, and the server replays unacked batches. The client
  stores no cursor and does not dedupe on `record_id`. Reconnect with the same
  consumer id.

  Option lists are `t:Diavasi.Client.Behaviour.start_options/0` and
  `t:Diavasi.Client.Behaviour.run_options/0`.

  ## Errors

  A failed handshake or read uses these strings:

    * `"protocol error N: message"` for codes 1 bad version, 2 bad state,
      3 unknown ack, 4 duplicate ack, 5 group not running, 6 unsupported,
      7 internal, 8 heartbeat timeout
    * `"grpc STATUS: message"` for a non-zero gRPC status. A rejected token is
      `"grpc 16: unauthorized"` when the server sends that trailer
    * `"http2 error \#{inspect(reason)}"` when the transport read fails
    * `"incomplete consume records=N batches=M"` from `run/1` when the stream
      ends before `:total` records
    * `"stream closed"` and `"unexpected frame ..."` during the handshake

  `start_link/1` returns a transport failure unchanged, for example
  `{:error, :econnrefused}`. `run/1` runs that failure through `inspect/1`,
  so the same failure is `{:error, ":econnrefused"}`.

  ## Mocks

  `put_client/1` installs another `Diavasi.Client.Behaviour`:

      Mox.defmock(MyApp.DiavasiMock, for: Diavasi.Client.Behaviour)
      Diavasi.Client.put_client(MyApp.DiavasiMock)

  `config :diavasi_client, client: MyApp.DiavasiMock` is the same switch.
  `reset_client/0` restores this module. Call `Mox.set_mox_global()` when the
  code under test runs in another process.

  `Diavasi.HTTP.put_client/1` replaces `Mint.HTTP` and keeps this client.
  That mock also needs `Mox.set_mox_global()`, because the GenServer owns the socket.
  """

  use GenServer, restart: :transient

  @behaviour Diavasi.Client.Behaviour

  alias Diavasi.HTTP
  alias Diavasi.Data.V1.{Ack, Envelope, FlowControl, Hello, JoinGroup, Leave}

  @path "/diavasi.data.v1.DataPlane/Consume"

  @doc """
  Open a client, complete the handshake, and link it to the caller.

  `opts` is a `t:Diavasi.Client.Behaviour.start_options/0`.

    * `:addr` - required `String.t()`, `host:port` of the data plane.
    * `:ca` - required `Path.t()`, data-plane CA file.
    * `:token` - required `String.t()`, bearer token.
    * `:group` - required `String.t()`, group id.
    * `:consumer` - optional `String.t()`, default `"elixir"`.
    * `:max_in_flight` - optional `non_neg_integer()`, default `1`.

  Returns `{:ok, pid}` after Hello, JoinGroup, and FlowControl.
  Returns `{:error, reason}` on connect or handshake failure. See the module
  docs for `reason`. Linking happens only after init succeeds, so a failure
  is a return value.

  A missing option or a bad `:addr` fails in `init/1`:

    * `{:error, {%KeyError{}, stacktrace}}` when a required key is missing
    * `{:error, {{:badmatch, parts}, stacktrace}}` when `:addr` is not `host:port`
    * `{:error, {:badarg, stacktrace}}` when the port is not an integer

  ## Examples

      iex> {:error, {%KeyError{key: :addr}, _}} = Diavasi.Client.start_link([])
      iex> {:error, {{:badmatch, ["localhost"]}, _}} =
      ...>   Diavasi.Client.start_link(addr: "localhost", ca: "ca.pem", token: "t", group: "g")
      iex> {:error, {:badarg, _}} =
      ...>   Diavasi.Client.start_link(addr: "127.0.0.1:nope", ca: "ca.pem", token: "t", group: "g")
      iex> Diavasi.Client.put_client(Diavasi.Client.Mock)
      :ok
      iex> Mox.stub(Diavasi.Client.Mock, :start_link, fn opts ->
      ...>   "127.0.0.1:7710" = opts[:addr]
      ...>   "/tmp/diavasi-sdk/dataplane-ca.crt" = opts[:ca]
      ...>   "sdk-demo" = opts[:token]
      ...>   "demo" = opts[:group]
      ...>   "elixir" = opts[:consumer]
      ...>   1 = opts[:max_in_flight]
      ...>   {:ok, :started}
      ...> end)
      iex> Diavasi.Client.start_link(
      ...>   addr: "127.0.0.1:7710",
      ...>   ca: "/tmp/diavasi-sdk/dataplane-ca.crt",
      ...>   token: "sdk-demo",
      ...>   group: "demo",
      ...>   consumer: "elixir",
      ...>   max_in_flight: 1
      ...> )
      {:ok, :started}
      iex> Mox.stub(Diavasi.Client.Mock, :start_link, fn _opts ->
      ...>   {:error, "protocol error 5: not running"}
      ...> end)
      iex> Diavasi.Client.start_link(addr: "127.0.0.1:7710", ca: "ca.pem", token: "t", group: "demo")
      {:error, "protocol error 5: not running"}
      iex> Diavasi.Client.reset_client()
      :ok
  """
  @spec start_link(Diavasi.Client.Behaviour.start_options()) :: GenServer.on_start()
  @impl Diavasi.Client.Behaviour
  def start_link(opts) when is_list(opts) do
    dispatch(:start_link, [opts], fn -> open(opts) end)
  end

  @doc """
  Install `module` for every `Diavasi.Client.Behaviour` callback.

  `module` is an atom and must not be this module. Returns `:ok`.
  Raises `FunctionClauseError` when `module` is `Diavasi.Client`.
  `reset_client/0` clears the setting.

  ## Examples

      iex> Diavasi.Client.put_client(Diavasi.Client.Mock)
      :ok
      iex> Mox.stub(Diavasi.Client.Mock, :run, fn opts -> {:ok, [opts[:total]], [1]} end)
      iex> Diavasi.Client.run(total: 2, group: "demo")
      {:ok, [2], [1]}
      iex> Diavasi.Client.reset_client()
      :ok
  """
  @spec put_client(module()) :: :ok
  def put_client(module) when is_atom(module) and module != __MODULE__ do
    Application.put_env(:diavasi_client, :client, module)
  end

  @doc """
  Clear `put_client/1` so later calls use this module.

  Returns `:ok`.

  ## Examples

      iex> Diavasi.Client.put_client(Diavasi.Client.Mock)
      :ok
      iex> Diavasi.Client.reset_client()
      :ok
      iex> Application.get_env(:diavasi_client, :client)
      nil
  """
  @spec reset_client() :: :ok
  def reset_client do
    Application.delete_env(:diavasi_client, :client)
    :ok
  end

  @doc """
  Stream batches from `pid`.

  Returns a stream of `Diavasi.Data.V1.RecordBatch`. Ack each `batch.batch_id`.
  The stream stops when `next_batch/1` returns `:done`.
  It raises `RuntimeError` when `next_batch/1` returns `{:error, reason}`.
  The exception message is `reason`.

  `pid` must be a pid. A non-pid raises `FunctionClauseError`.

  ## Examples

      iex> batch = %Diavasi.Data.V1.RecordBatch{
      ...>   batch_id: 7,
      ...>   records: [%Diavasi.Data.V1.Record{record_id: 1, payload: "a"}]
      ...> }
      iex> Diavasi.Client.put_client(Diavasi.Client.Mock)
      :ok
      iex> Mox.stub(Diavasi.Client.Mock, :stream, fn pid ->
      ...>   true = is_pid(pid)
      ...>   [batch]
      ...> end)
      iex> [got] = self() |> Diavasi.Client.stream() |> Enum.to_list()
      iex> {got.batch_id, hd(got.records).payload}
      {7, "a"}
      iex> Mox.stub(Diavasi.Client.Mock, :stream, fn _pid ->
      ...>   raise "protocol error 5: not running"
      ...> end)
      iex> try do
      ...>   self() |> Diavasi.Client.stream() |> Enum.to_list()
      ...> rescue
      ...>   RuntimeError -> :raised
      ...> end
      :raised
      iex> Diavasi.Client.reset_client()
      :ok
  """
  @spec stream(pid()) :: Enumerable.t()
  @impl Diavasi.Client.Behaviour
  def stream(pid) when is_pid(pid) do
    dispatch(:stream, [pid], fn -> stream_batches(pid) end)
  end

  defp stream_batches(pid) do
    Stream.resource(
      fn -> :open end,
      fn
        :open ->
          case next_batch(pid) do
            {:ok, batch} -> {[batch], :open}
            :done -> {:halt, :open}
            {:error, reason} -> raise reason
          end

        other ->
          {:halt, other}
      end,
      fn _ -> :ok end
    )
  end

  @doc """
  Take the next batch from `pid`.

  Returns `{:ok, batch}` where `batch` is a `Diavasi.Data.V1.RecordBatch`,
  `:done` when the stream has ended, or `{:error, reason}` with a string
  from the module docs.

  The call waits up to 60 seconds, then exits with `:timeout`.
  It exits with `:noproc` when `pid` is not alive.

  ## Examples

      iex> Diavasi.Client.put_client(Diavasi.Client.Mock)
      :ok
      iex> Mox.stub(Diavasi.Client.Mock, :next_batch, fn _pid ->
      ...>   {:ok, %Diavasi.Data.V1.RecordBatch{batch_id: 4, records: []}}
      ...> end)
      iex> {:ok, batch} = Diavasi.Client.next_batch(self())
      iex> batch.batch_id
      4
      iex> Mox.stub(Diavasi.Client.Mock, :next_batch, fn _pid -> :done end)
      iex> Diavasi.Client.next_batch(self())
      :done
      iex> Mox.stub(Diavasi.Client.Mock, :next_batch, fn _pid ->
      ...>   {:error, "protocol error 5: not running"}
      ...> end)
      iex> Diavasi.Client.next_batch(self())
      {:error, "protocol error 5: not running"}
      iex> Diavasi.Client.reset_client()
      :ok
  """
  @spec next_batch(pid()) ::
          {:ok, Diavasi.Data.V1.RecordBatch.t()} | :done | {:error, String.t()}
  @impl Diavasi.Client.Behaviour
  def next_batch(pid) do
    dispatch(:next_batch, [pid], fn -> GenServer.call(pid, :next_batch, 60_000) end)
  end

  @doc """
  Ack `batch_id` on `pid`.

  `batch_id` is the `batch_id` field of a `Diavasi.Data.V1.RecordBatch`.
  Returns `:ok` after the Ack frame is written. The client does not wait for
  a server reply.

  Exits with `:noproc` when `pid` is not alive, and with `:timeout` after 5 seconds.

  ## Examples

      iex> pid = spawn(fn -> :ok end)
      iex> ref = Process.monitor(pid)
      iex> receive do
      ...>   {:DOWN, ^ref, :process, ^pid, _} -> :down
      ...> end
      :down
      iex> {:noproc, _} = catch_exit(Diavasi.Client.ack(pid, 1))
      iex> Diavasi.Client.put_client(Diavasi.Client.Mock)
      :ok
      iex> Mox.stub(Diavasi.Client.Mock, :ack, fn _pid, 7 -> :ok end)
      iex> Diavasi.Client.ack(self(), 7)
      :ok
      iex> Diavasi.Client.reset_client()
      :ok
  """
  @spec ack(pid(), non_neg_integer()) :: :ok
  @impl Diavasi.Client.Behaviour
  def ack(pid, batch_id) do
    dispatch(:ack, [pid, batch_id], fn -> GenServer.call(pid, {:ack, batch_id}) end)
  end

  @doc """
  Send Leave on `pid` and half-close the request stream.

  Returns `:ok`. Unacked batches are not replayed after a successful Leave.
  Exits with `:noproc` when `pid` is not alive, and with `:timeout` after 5 seconds.
  If the transport does not return `{:ok, conn}` for `:eof`, the process exits
  and this call exits with that reason.

  ## Examples

      iex> Diavasi.Client.put_client(Diavasi.Client.Mock)
      :ok
      iex> Mox.stub(Diavasi.Client.Mock, :leave, fn pid ->
      ...>   true = is_pid(pid)
      ...>   :ok
      ...> end)
      iex> Diavasi.Client.leave(self())
      :ok
      iex> Diavasi.Client.reset_client()
      :ok
  """
  @spec leave(pid()) :: :ok
  @impl Diavasi.Client.Behaviour
  def leave(pid) do
    dispatch(:leave, [pid], fn -> GenServer.call(pid, :leave) end)
  end

  @doc """
  Stop `pid` without sending Leave.

  Returns `:ok`. The server replays batches that were not acked.
  A `pid` that has already stopped is also `:ok`.

  ## Examples

      iex> pid = spawn(fn -> :ok end)
      iex> ref = Process.monitor(pid)
      iex> receive do
      ...>   {:DOWN, ^ref, :process, ^pid, _} -> :down
      ...> end
      :down
      iex> Diavasi.Client.disconnect(pid)
      :ok
      iex> Diavasi.Client.put_client(Diavasi.Client.Mock)
      :ok
      iex> Mox.stub(Diavasi.Client.Mock, :disconnect, fn _pid -> :ok end)
      iex> Diavasi.Client.disconnect(self())
      :ok
      iex> Diavasi.Client.reset_client()
      :ok
  """
  @spec disconnect(pid()) :: :ok
  @impl Diavasi.Client.Behaviour
  def disconnect(pid) do
    dispatch(:disconnect, [pid], fn ->
      if Process.alive?(pid), do: GenServer.stop(pid, :normal)
      :ok
    end)
  end

  @doc """
  Ack `:total` records and then Leave.

  `opts` is a `t:Diavasi.Client.Behaviour.run_options/0`. It accepts every
  `start_link/1` option, plus:

    * `:total` - required `non_neg_integer()`, records to ack.
    * `:halt_after` - optional `non_neg_integer()`. After this many acks the
      client reads once more, waiting up to 5 seconds, ignores an exit from
      that read, then `disconnect/1`s. Leave is not sent.

  Returns `{:ok, record_ids, batch_ids}`. Both lists are non-negative integers
  in ack order. Returns `{:error, reason}` on handshake failure, a short
  stream (`"incomplete consume records=N batches=M"`), or a later read error.
  Transport errors are `inspect/1` strings.

  Raises `KeyError` when `:total` is missing. A bad address fails inside
  `start_link/1` and is returned as `{:error, reason}` with the shapes
  documented on `start_link/1`.

  ## Examples

      iex> try do
      ...>   Diavasi.Client.run([])
      ...> rescue
      ...>   KeyError -> :missing_total
      ...> end
      :missing_total
      iex> Diavasi.Client.put_client(Diavasi.Client.Mock)
      :ok
      iex> Mox.stub(Diavasi.Client.Mock, :run, fn opts ->
      ...>   8 = opts[:total]
      ...>   1 = opts[:halt_after]
      ...>   "demo" = opts[:group]
      ...>   {:ok, [1, 2], [4]}
      ...> end)
      iex> Diavasi.Client.run(
      ...>   addr: "127.0.0.1:7710",
      ...>   ca: "/tmp/diavasi-sdk/dataplane-ca.crt",
      ...>   token: "sdk-demo",
      ...>   group: "demo",
      ...>   total: 8,
      ...>   halt_after: 1
      ...> )
      {:ok, [1, 2], [4]}
      iex> Mox.stub(Diavasi.Client.Mock, :run, fn _opts ->
      ...>   {:error, "incomplete consume records=0 batches=0"}
      ...> end)
      iex> Diavasi.Client.run(total: 8, group: "demo")
      {:error, "incomplete consume records=0 batches=0"}
      iex> Diavasi.Client.reset_client()
      :ok
  """
  @spec run(Diavasi.Client.Behaviour.run_options()) ::
          {:ok, [non_neg_integer()], [non_neg_integer()]} | {:error, String.t()}
  @impl Diavasi.Client.Behaviour
  def run(opts) do
    dispatch(:run, [opts], fn -> consume(opts) end)
  end

  defp open(opts) do
    # Link after init so a refused connection or a failed handshake
    # comes back as {:error, reason}.
    case GenServer.start(__MODULE__, opts) do
      {:ok, pid} ->
        Process.link(pid)
        {:ok, pid}

      other ->
        other
    end
  end

  defp consume(opts) do
    total = Keyword.fetch!(opts, :total)
    halt_after = Keyword.get(opts, :halt_after)

    case start_link(opts) do
      {:ok, pid} ->
        try do
          take(pid, total, halt_after, [], [])
        after
          if Process.alive?(pid), do: GenServer.stop(pid)
        end

      {:error, reason} ->
        {:error, format_error(reason)}
    end
  end

  @impl GenServer
  def init(opts) do
    addr = Keyword.fetch!(opts, :addr)
    ca = Keyword.fetch!(opts, :ca)
    token = Keyword.fetch!(opts, :token)
    group = Keyword.fetch!(opts, :group)
    consumer = Keyword.get(opts, :consumer, "elixir")
    max_in_flight = Keyword.get(opts, :max_in_flight, 1)

    [host, port_s] = String.split(addr, ":")
    port = String.to_integer(port_s)

    with {:ok, conn} <-
           http().connect(:https, host, port,
             protocols: [:http2],
             mode: :passive,
             transport_opts: [
               verify: :verify_peer,
               cacertfile: String.to_charlist(ca),
               server_name_indication: ~c"localhost"
             ]
           ),
         {:ok, conn, ref} <-
           http().request(
             conn,
             "POST",
             @path,
             [
               {"content-type", "application/grpc"},
               {"te", "trailers"},
               {"authorization", "Bearer #{token}"}
             ],
             :stream
           ) do
      state = %{
        conn: conn,
        ref: ref,
        buffer: <<>>,
        pending: [],
        closed: false,
        group: group,
        consumer: consumer,
        max_in_flight: max_in_flight,
        sent_flow: false,
        left: false
      }

      state = send_env(state, hello())

      case handshake(state) do
        {:ok, state} ->
          {:ok, state}

        {:error, reason, state} ->
          http().close(state.conn)
          {:stop, format_error(reason)}
      end
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl GenServer
  def handle_call(:next_batch, _from, state) do
    case pull(state) do
      {:batch, batch, state} -> {:reply, {:ok, batch}, state}
      {:done, state} -> {:reply, :done, state}
      {:error, reason, state} -> {:reply, {:error, format_error(reason)}, state}
    end
  end

  @impl GenServer
  def handle_call({:ack, batch_id}, _from, state) do
    {:reply, :ok, send_env(state, %Envelope{version: 1, body: {:ack, %Ack{batch_id: batch_id}}})}
  end

  @impl GenServer
  def handle_call(:leave, _from, state) do
    state = send_env(state, %Envelope{version: 1, body: {:leave, %Leave{}}})
    {:ok, conn} = http().stream_request_body(state.conn, state.ref, :eof)
    {:reply, :ok, %{state | conn: conn, left: true}}
  end

  @impl GenServer
  def terminate(_reason, %{conn: conn}) do
    http().close(conn)
    :ok
  end

  def terminate(_reason, _state), do: :ok

  defp take(pid, total, halt_after, record_ids, batch_ids) do
    if length(record_ids) >= total do
      :ok = leave(pid)
      {:ok, record_ids, batch_ids}
    else
      case next_batch(pid) do
        {:ok, batch} ->
          accept_batch(pid, total, halt_after, record_ids, batch_ids, batch)

        :done ->
          {:error,
           "incomplete consume records=#{length(record_ids)} batches=#{length(batch_ids)}"}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp accept_batch(pid, total, halt_after, record_ids, batch_ids, batch) do
    ids = Enum.map(batch.records, & &1.record_id)
    :ok = ack(pid, batch.batch_id)
    batch_ids = batch_ids ++ [batch.batch_id]
    record_ids = record_ids ++ ids

    if is_integer(halt_after) and length(batch_ids) >= halt_after do
      halt(pid, record_ids, batch_ids)
    else
      take(pid, total, halt_after, record_ids, batch_ids)
    end
  end

  defp halt(pid, record_ids, batch_ids) do
    try do
      GenServer.call(pid, :next_batch, 5_000)
    catch
      :exit, _ -> :ok
    end

    disconnect(pid)
    {:ok, record_ids, batch_ids}
  end

  defp handshake(state) do
    case pull_until(state, :joined) do
      {:ok, state} -> {:ok, state}
      other -> other
    end
  end

  defp pull_until(state, :joined) do
    case recv_events(state) do
      {:error, reason, state} ->
        {:error, reason, state}

      {:events, events, state} ->
        case apply_handshake(events, state) do
          {:joined, state} -> {:ok, state}
          {:continue, state} -> pull_until(state, :joined)
          {:error, reason, state} -> {:error, reason, state}
        end
    end
  end

  defp apply_handshake([], state), do: {:continue, state}

  defp apply_handshake([{:hello_ack, _} | rest], state) do
    state =
      send_env(
        state,
        %Envelope{
          version: 1,
          body: {:join_group, %JoinGroup{group_id: state.group, consumer_id: state.consumer}}
        }
      )

    apply_handshake(rest, state)
  end

  defp apply_handshake([{:joined, _} | rest], %{sent_flow: false} = state) do
    state = %{state | sent_flow: true}

    state =
      send_env(
        state,
        %Envelope{
          version: 1,
          body: {:flow_control, %FlowControl{max_in_flight: state.max_in_flight}}
        }
      )

    {batches, state, terminal} = collect(rest, state, [])

    state =
      state
      |> Map.put(:pending, Enum.reverse(batches))
      |> Map.put(:closed, terminal == :done)

    case terminal do
      {:error, reason} -> {:error, reason, state}
      _ -> {:joined, state}
    end
  end

  defp apply_handshake([{:heartbeat, _} | rest], state) do
    apply_handshake(rest, send_env(state, heartbeat()))
  end

  defp apply_handshake([{:error, err} | _], state) do
    {:error, {:protocol, err.code, err.message}, state}
  end

  defp apply_handshake([{:grpc, status, message} | _], state) do
    {:error, {:grpc, status, message}, state}
  end

  defp apply_handshake([{:done, _} | _], state), do: {:error, "stream closed", state}

  defp apply_handshake([other | _], state) do
    {:error, "unexpected frame #{inspect(other)}", state}
  end

  defp pull(%{closed: true, pending: []} = state), do: {:done, state}

  defp pull(%{pending: [batch | rest]} = state) do
    {:batch, batch, %{state | pending: rest}}
  end

  defp pull(state) do
    case recv_events(state) do
      {:error, reason, state} ->
        {:error, reason, state}

      {:events, events, state} ->
        {batches, state, terminal} = collect(events, state, [])
        state = if terminal == :done, do: %{state | closed: true}, else: state

        cond do
          match?({:error, _}, terminal) ->
            {:error, elem(terminal, 1), state}

          batches != [] ->
            [batch | rest] = Enum.reverse(batches)
            {:batch, batch, %{state | pending: rest}}

          terminal == :done ->
            {:done, state}

          true ->
            pull(state)
        end
    end
  end

  defp collect([], state, batches), do: {batches, state, nil}

  defp collect([{:record_batch, batch} | rest], state, batches) do
    collect(rest, state, [batch | batches])
  end

  defp collect([{:heartbeat, _} | rest], state, batches) do
    collect(rest, send_env(state, heartbeat()), batches)
  end

  defp collect([{:error, err} | _], state, _batches) do
    {[], state, {:error, {:protocol, err.code, err.message}}}
  end

  defp collect([{:grpc, status, message} | _], state, _batches) do
    {[], state, {:error, {:grpc, status, message}}}
  end

  defp collect([{:done, _} | _], state, batches), do: {batches, state, :done}

  defp collect([_ | rest], state, batches), do: collect(rest, state, batches)

  defp recv_events(state) do
    case http().recv(state.conn, 0, 30_000) do
      {:ok, conn, responses} ->
        state = %{state | conn: conn}

        {events, state} =
          Enum.reduce(responses, {[], state}, fn response, {events, state} ->
            {more, state} = events_from(response, state)
            {events ++ more, state}
          end)

        {:events, events, state}

      {:error, conn, reason, _} ->
        {:error, "http2 error #{inspect(reason)}", %{state | conn: conn}}
    end
  end

  defp events_from({:status, _ref, status}, state) when status >= 400 do
    {[{:grpc, Integer.to_string(status), ""}], state}
  end

  defp events_from({:data, _ref, data}, state) do
    {frames, rest} = take_frames(state.buffer <> data, [])
    state = %{state | buffer: rest}
    {Enum.map(frames, &frame_event/1), state}
  end

  defp events_from({:headers, _ref, headers}, state) do
    case List.keyfind(headers, "grpc-status", 0) do
      {_, "0"} ->
        {[], state}

      {_, status} ->
        message =
          case List.keyfind(headers, "grpc-message", 0) do
            {_, msg} -> URI.decode(msg)
            nil -> ""
          end

        {[{:grpc, status, message}], state}

      nil ->
        {[], state}
    end
  end

  defp events_from({:done, _ref}, state), do: {[{:done, true}], state}
  defp events_from(_other, state), do: {[], state}

  defp frame_event(%Envelope{body: {:hello_ack, ack}}), do: {:hello_ack, ack}
  defp frame_event(%Envelope{body: {:joined, joined}}), do: {:joined, joined}
  defp frame_event(%Envelope{body: {:record_batch, batch}}), do: {:record_batch, batch}
  defp frame_event(%Envelope{body: {:heartbeat, beat}}), do: {:heartbeat, beat}
  defp frame_event(%Envelope{body: {:error, err}}), do: {:error, err}
  defp frame_event(other), do: {:other, other}

  defp hello, do: %Envelope{version: 1, body: {:hello, %Hello{protocol_version: 1}}}
  defp heartbeat, do: %Envelope{version: 1, body: {:heartbeat, %Diavasi.Data.V1.Heartbeat{}}}

  defp send_env(%{conn: conn, ref: ref} = state, envelope) do
    {:ok, conn} = http().stream_request_body(conn, ref, frame(envelope))
    %{state | conn: conn}
  end

  defp frame(envelope) do
    bin = Envelope.encode(envelope)
    <<0, byte_size(bin)::32-big, bin::binary>>
  end

  defp take_frames(<<_flag, len::32-big, rest::binary>>, acc) when byte_size(rest) >= len do
    <<payload::binary-size(^len), rest::binary>> = rest
    take_frames(rest, [Envelope.decode(payload) | acc])
  end

  defp take_frames(buffer, acc), do: {Enum.reverse(acc), buffer}

  defp dispatch(name, args, fun) do
    case Application.get_env(:diavasi_client, :client) do
      nil -> fun.()
      module -> apply(module, name, args)
    end
  end

  defp http, do: HTTP.client()

  defp format_error({:protocol, code, message}), do: "protocol error #{code}: #{message}"
  defp format_error({:grpc, status, message}), do: "grpc #{status}: #{message}"
  defp format_error(other), do: to_string_error(other)

  defp to_string_error(reason) when is_binary(reason), do: reason
  defp to_string_error(reason), do: inspect(reason)
end
