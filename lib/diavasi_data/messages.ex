defmodule Diavasi.Data.V1.Hello do
  @moduledoc """
  First frame on `diavasi.data.v1`. The client sends it after the HTTP/2 request starts.

  ## Fields

    * `protocol_version` - `non_neg_integer()`. This client sends `1`.

  `encode/1` returns protobuf bytes. `decode/1` returns the struct.
  Decoded structs also carry `__unknown_fields__` and `__protobuf__`.

  ## Examples

      iex> hello = %Diavasi.Data.V1.Hello{protocol_version: 1}
      iex> decoded = Diavasi.Data.V1.Hello.decode(Diavasi.Data.V1.Hello.encode(hello))
      iex> decoded.protocol_version
      1
  """

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field(:protocol_version, 1, type: :uint32, json_name: "protocolVersion")
end

defmodule Diavasi.Data.V1.HelloAck do
  @moduledoc """
  Server reply to `Diavasi.Data.V1.Hello`. The client then sends `Diavasi.Data.V1.JoinGroup`.

  ## Fields

    * `protocol_version` - `non_neg_integer()`. `1` for this protocol.

  ## Examples

      iex> ack = %Diavasi.Data.V1.HelloAck{protocol_version: 1}
      iex> decoded = Diavasi.Data.V1.HelloAck.decode(Diavasi.Data.V1.HelloAck.encode(ack))
      iex> decoded.protocol_version
      1
  """

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field(:protocol_version, 1, type: :uint32, json_name: "protocolVersion")
end

defmodule Diavasi.Data.V1.JoinGroup do
  @moduledoc """
  Client request to join a running group.

  ## Fields

    * `group_id` - `String.t()`. Group id from `Diavasi.Data.Client.start_link/1`.
    * `consumer_id` - `String.t()`. Consumer id. The client default is `"elixir"`.

  ## Examples

      iex> join = %Diavasi.Data.V1.JoinGroup{group_id: "demo", consumer_id: "elixir"}
      iex> decoded = Diavasi.Data.V1.JoinGroup.decode(Diavasi.Data.V1.JoinGroup.encode(join))
      iex> {decoded.group_id, decoded.consumer_id}
      {"demo", "elixir"}
  """

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field(:group_id, 1, type: :string, json_name: "groupId")
  field(:consumer_id, 2, type: :string, json_name: "consumerId")
end

defmodule Diavasi.Data.V1.Joined do
  @moduledoc """
  Server reply to `Diavasi.Data.V1.JoinGroup`. The client then sends one `Diavasi.Data.V1.FlowControl`.

  ## Fields

    * `group_id` - `String.t()`.
    * `consumer_id` - `String.t()`.

  ## Examples

      iex> joined = %Diavasi.Data.V1.Joined{group_id: "demo", consumer_id: "elixir"}
      iex> decoded = Diavasi.Data.V1.Joined.decode(Diavasi.Data.V1.Joined.encode(joined))
      iex> {decoded.group_id, decoded.consumer_id}
      {"demo", "elixir"}
  """

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field(:group_id, 1, type: :string, json_name: "groupId")
  field(:consumer_id, 2, type: :string, json_name: "consumerId")
end

defmodule Diavasi.Data.V1.Record do
  @moduledoc """
  One record inside a `Diavasi.Data.V1.RecordBatch`.

  ## Fields

    * `record_id` - `non_neg_integer()`. Id assigned by the server. The client does not dedupe on it.
    * `payload` - `binary()`. Record bytes. Empty is valid.

  ## Examples

      iex> record = %Diavasi.Data.V1.Record{record_id: 1, payload: "hi"}
      iex> decoded = Diavasi.Data.V1.Record.decode(Diavasi.Data.V1.Record.encode(record))
      iex> {decoded.record_id, decoded.payload}
      {1, "hi"}
  """

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field(:record_id, 1, type: :uint64, json_name: "recordId")
  field(:payload, 2, type: :bytes)
end

defmodule Diavasi.Data.V1.RecordBatch do
  @moduledoc """
  A batch of records. `Diavasi.Data.Client.next_batch/1` and `stream/1` yield this struct.

  ## Fields

    * `batch_id` - `non_neg_integer()`. Pass this to `Diavasi.Data.Client.ack/2`.
    * `records` - list of `Diavasi.Data.V1.Record`. May be empty.

  ## Examples

      iex> batch = %Diavasi.Data.V1.RecordBatch{
      ...>   batch_id: 3,
      ...>   records: [%Diavasi.Data.V1.Record{record_id: 1, payload: "hi"}]
      ...> }
      iex> decoded = Diavasi.Data.V1.RecordBatch.decode(Diavasi.Data.V1.RecordBatch.encode(batch))
      iex> {decoded.batch_id, hd(decoded.records).record_id}
      {3, 1}
  """

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field(:batch_id, 1, type: :uint64, json_name: "batchId")
  field(:records, 2, repeated: true, type: Diavasi.Data.V1.Record)
end

defmodule Diavasi.Data.V1.Ack do
  @moduledoc """
  Client acknowledgement of one `Diavasi.Data.V1.RecordBatch`.

  ## Fields

    * `batch_id` - `non_neg_integer()`. The `batch_id` from the batch.

  ## Examples

      iex> ack = %Diavasi.Data.V1.Ack{batch_id: 3}
      iex> decoded = Diavasi.Data.V1.Ack.decode(Diavasi.Data.V1.Ack.encode(ack))
      iex> decoded.batch_id
      3
  """

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field(:batch_id, 1, type: :uint64, json_name: "batchId")
end

defmodule Diavasi.Data.V1.Nack do
  @moduledoc """
  Reserved batch rejection. `Diavasi.Data.Client` does not send this message.

  ## Fields

    * `batch_id` - `non_neg_integer()`.

  ## Examples

      iex> nack = %Diavasi.Data.V1.Nack{batch_id: 3}
      iex> decoded = Diavasi.Data.V1.Nack.decode(Diavasi.Data.V1.Nack.encode(nack))
      iex> decoded.batch_id
      3
  """

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field(:batch_id, 1, type: :uint64, json_name: "batchId")
end

defmodule Diavasi.Data.V1.Heartbeat do
  @moduledoc """
  Empty keepalive. Either side may send it. The client answers a server heartbeat with another heartbeat.

  This message has no fields.

  ## Examples

      iex> beat = %Diavasi.Data.V1.Heartbeat{}
      iex> decoded = Diavasi.Data.V1.Heartbeat.decode(Diavasi.Data.V1.Heartbeat.encode(beat))
      iex> match?(%Diavasi.Data.V1.Heartbeat{}, decoded)
      true
  """

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3
end

defmodule Diavasi.Data.V1.FlowControl do
  @moduledoc """
  Client limit on unacked batches. `Diavasi.Data.Client` sends this once, after `Diavasi.Data.V1.Joined`.

  ## Fields

    * `max_in_flight` - `non_neg_integer()`. The `:max_in_flight` option, default `1`.

  ## Examples

      iex> flow = %Diavasi.Data.V1.FlowControl{max_in_flight: 1}
      iex> decoded = Diavasi.Data.V1.FlowControl.decode(Diavasi.Data.V1.FlowControl.encode(flow))
      iex> decoded.max_in_flight
      1
  """

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field(:max_in_flight, 1, type: :uint32, json_name: "maxInFlight")
end

defmodule Diavasi.Data.V1.ErrorMessage do
  @moduledoc """
  Protocol error on the stream. The client stops the handshake or the read and returns the text to the caller.

  ## Fields

    * `code` - `non_neg_integer()`.
    * `message` - `String.t()`.

  Codes:

    * `1` bad version
    * `2` bad state
    * `3` unknown ack
    * `4` duplicate ack
    * `5` group not running
    * `6` unsupported
    * `7` internal
    * `8` heartbeat timeout

  The client formats these as `"protocol error \#{code}: \#{message}"`.

  ## Examples

      iex> err = %Diavasi.Data.V1.ErrorMessage{code: 5, message: "not running"}
      iex> decoded = Diavasi.Data.V1.ErrorMessage.decode(Diavasi.Data.V1.ErrorMessage.encode(err))
      iex> {decoded.code, decoded.message}
      {5, "not running"}
  """

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field(:code, 1, type: :uint32)
  field(:message, 2, type: :string)
end

defmodule Diavasi.Data.V1.Leave do
  @moduledoc """
  Client end of the stream. `Diavasi.Data.Client.leave/1` sends this, then half-closes the request.

  This message has no fields. Dropping the process, or `Diavasi.Data.Client.disconnect/1`,
  skips Leave. The server then replays unacked batches.

  ## Examples

      iex> leave = %Diavasi.Data.V1.Leave{}
      iex> decoded = Diavasi.Data.V1.Leave.decode(Diavasi.Data.V1.Leave.encode(leave))
      iex> match?(%Diavasi.Data.V1.Leave{}, decoded)
      true
  """

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3
end

defmodule Diavasi.Data.V1.Envelope do
  @moduledoc """
  One data-plane frame. `version` is `1`. `body` is a oneof: exactly one of the messages below.

  ## Fields

    * `version` - `non_neg_integer()`. Protocol version. The client sends `1`.
    * `body` - one of:
      * `{:hello, Diavasi.Data.V1.Hello.t()}`
      * `{:hello_ack, Diavasi.Data.V1.HelloAck.t()}`
      * `{:join_group, Diavasi.Data.V1.JoinGroup.t()}`
      * `{:joined, Diavasi.Data.V1.Joined.t()}`
      * `{:record_batch, Diavasi.Data.V1.RecordBatch.t()}`
      * `{:ack, Diavasi.Data.V1.Ack.t()}`
      * `{:nack, Diavasi.Data.V1.Nack.t()}`
      * `{:heartbeat, Diavasi.Data.V1.Heartbeat.t()}`
      * `{:flow_control, Diavasi.Data.V1.FlowControl.t()}`
      * `{:error, Diavasi.Data.V1.ErrorMessage.t()}`
      * `{:leave, Diavasi.Data.V1.Leave.t()}`

  On the wire the client prefixes `encode/1` with a gRPC frame: `<<0, byte_size::32-big, bytes::binary>>`.

  ## Examples

      iex> envelope = %Diavasi.Data.V1.Envelope{
      ...>   version: 1,
      ...>   body: {:hello, %Diavasi.Data.V1.Hello{protocol_version: 1}}
      ...> }
      iex> decoded = Diavasi.Data.V1.Envelope.decode(Diavasi.Data.V1.Envelope.encode(envelope))
      iex> decoded.version
      1
      iex> {:hello, hello} = decoded.body
      iex> hello.protocol_version
      1
  """

  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field(:version, 1, type: :uint32)

  oneof(:body, 0)

  field(:hello, 2, type: Diavasi.Data.V1.Hello, oneof: 0)
  field(:hello_ack, 3, type: Diavasi.Data.V1.HelloAck, json_name: "helloAck", oneof: 0)
  field(:join_group, 4, type: Diavasi.Data.V1.JoinGroup, json_name: "joinGroup", oneof: 0)
  field(:joined, 5, type: Diavasi.Data.V1.Joined, oneof: 0)
  field(:record_batch, 6, type: Diavasi.Data.V1.RecordBatch, json_name: "recordBatch", oneof: 0)
  field(:ack, 7, type: Diavasi.Data.V1.Ack, oneof: 0)
  field(:nack, 8, type: Diavasi.Data.V1.Nack, oneof: 0)
  field(:heartbeat, 9, type: Diavasi.Data.V1.Heartbeat, oneof: 0)
  field(:flow_control, 10, type: Diavasi.Data.V1.FlowControl, json_name: "flowControl", oneof: 0)
  field(:error, 11, type: Diavasi.Data.V1.ErrorMessage, oneof: 0)
  field(:leave, 12, type: Diavasi.Data.V1.Leave, oneof: 0)
end
