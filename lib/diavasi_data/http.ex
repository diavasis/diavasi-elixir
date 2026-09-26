defmodule Diavasi.Data.HTTP do
  @moduledoc """
  HTTP/2 transport used by `Diavasi.Data.Client`. The default module is `Mint.HTTP`.

  `put_client/1` installs another module with these callbacks. The GenServer
  calls it, so a Mox mock needs `Mox.set_mox_global()`:

      Mox.defmock(MyApp.DiavasiHTTP, for: Diavasi.Data.HTTP)
      Diavasi.Data.HTTP.put_client(MyApp.DiavasiHTTP)
      Mox.set_mox_global()

  `reset_client/0` restores `Mint.HTTP`.

  Service tests that should skip the network call `Diavasi.Data.Client.put_client/1`
  and replace the whole client. Use this behaviour when the test runs
  `Diavasi.Data.Client` against a scripted connection.

  A double that records the calls:

      defmodule MyApp.DiavasiHTTP do
        @behaviour Diavasi.Data.HTTP

        @impl true
        def connect(scheme, address, port, opts), do: {:ok, {scheme, address, port, opts}}

        @impl true
        def request(conn, method, path, headers, :stream), do: {:ok, conn, {method, path, headers}}

        @impl true
        def stream_request_body(conn, _ref, _body), do: {:ok, conn}

        @impl true
        def recv(conn, _byte_count, _timeout), do: {:ok, conn, []}

        @impl true
        def close(conn), do: {:ok, conn}
      end
  """

  @typedoc """
  Transport connection returned by `c:connect/4` and passed to the other callbacks.

  `Mint.HTTP` uses its own connection struct. A test double can use any term.
  """
  @type conn :: term()

  @typedoc """
  Request reference returned by `c:request/5`.

  `Mint.HTTP` returns a `reference`. A test double can return any term.
  """
  @type request_ref :: term()

  @doc """
  Open `scheme` to `address` at `port`.

  `opts` is a keyword list. `Diavasi.Data.Client` passes
  `protocols: [:http2]`, `mode: :passive`, and `transport_opts` with
  `verify: :verify_peer`, `cacertfile` set from `:ca`, and
  `server_name_indication: ~c"localhost"`.

  Returns `{:ok, conn}` or `{:error, reason}`.

  ## Examples

      def connect(_scheme, _address, _port, _opts), do: {:ok, :conn}
  """
  @callback connect(
              scheme :: atom(),
              address :: String.t(),
              port :: :inet.port_number(),
              opts :: keyword()
            ) ::
              {:ok, conn()} | {:error, term()}

  @doc """
  Start a request on `conn`.

  `Diavasi.Data.Client` calls this once, with `method` `"POST"`, `path`
  `"/diavasi.data.v1.DataPlane/Consume"`, gRPC headers, and `body` `:stream`.
  Later frames go through `c:stream_request_body/3`.

  Returns `{:ok, conn, ref}` or `{:error, reason}`.

  ## Examples

      def request(conn, "POST", _path, _headers, :stream), do: {:ok, conn, make_ref()}
  """
  @callback request(
              conn(),
              method :: String.t(),
              path :: String.t(),
              headers :: [{String.t(), String.t()}],
              body :: :stream
            ) ::
              {:ok, conn(), request_ref()} | {:error, term()}

  @doc """
  Write one chunk of the request body.

  `body` is an iodata protobuf frame, or `:eof` after Leave.
  Returns `{:ok, conn}` or `{:error, reason}`. The client matches on
  `{:ok, conn}`. Any other return exits the process, and the caller of
  `Diavasi.Data.Client` exits with that reason.

  ## Examples

      def stream_request_body(conn, _ref, _body), do: {:ok, conn}
  """
  @callback stream_request_body(conn(), request_ref(), body :: iodata() | :eof) ::
              {:ok, conn()} | {:error, term()}

  @doc """
  Read the next HTTP/2 messages.

  `Diavasi.Data.Client` passes `byte_count` `0` and `timeout` `30_000`.
  Success is `{:ok, conn, messages}`. Messages the client understands are
  `{:status, ref, status}`, `{:data, ref, binary}`, `{:headers, ref, headers}`,
  and `{:done, ref}`.

  Failure is `{:error, conn, reason, messages}`. The client reports that as
  `"http2 error \#{inspect(reason)}"`.

  ## Examples

      def recv(conn, _byte_count, _timeout), do: {:ok, conn, []}
  """
  @callback recv(conn(), byte_count :: non_neg_integer(), timeout()) ::
              {:ok, conn(), messages :: list()}
              | {:error, conn(), reason :: term(), messages :: list()}

  @doc """
  Close `conn`.

  The return value is ignored. `Mint.HTTP.close/1` returns `{:ok, conn}`.

  ## Examples

      def close(_conn), do: :ok
  """
  @callback close(conn()) :: term()

  @doc """
  Install `module` as the transport.

  `module` must implement this behaviour. The default is `Mint.HTTP`, used
  when this key is unset.

  ## Examples

      iex> Diavasi.Data.HTTP.put_client(Diavasi.Data.HTTP.Mock)
      :ok
      iex> Diavasi.Data.HTTP.client()
      Diavasi.Data.HTTP.Mock
      iex> Diavasi.Data.HTTP.reset_client()
      :ok
  """
  @spec put_client(module()) :: :ok
  def put_client(module) when is_atom(module) do
    Application.put_env(:diavasi, :http_client, module)
  end

  @doc """
  Restore `Mint.HTTP`.

  ## Examples

      iex> Diavasi.Data.HTTP.put_client(Diavasi.Data.HTTP.Mock)
      :ok
      iex> Diavasi.Data.HTTP.reset_client()
      :ok
      iex> Diavasi.Data.HTTP.client()
      Mint.HTTP
  """
  @spec reset_client() :: :ok
  def reset_client do
    Application.delete_env(:diavasi, :http_client)
    :ok
  end

  @doc """
  Return the transport module.

  ## Examples

      iex> Diavasi.Data.HTTP.reset_client()
      :ok
      iex> Diavasi.Data.HTTP.client()
      Mint.HTTP
  """
  @spec client() :: module()
  def client, do: Application.get_env(:diavasi, :http_client, Mint.HTTP)
end
