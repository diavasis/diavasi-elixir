defmodule DiavasiBench.Envelope do
  @moduledoc """
  One Stage 0 bench frame. `version` is `1`. `body` is one of
  `{:join_group, DiavasiBench.JoinGroup.t()}`,
  `{:joined, DiavasiBench.Joined.t()}`,
  `{:record_batch, DiavasiBench.RecordBatch.t()}`,
  `{:ack, DiavasiBench.Ack.t()}`,
  `{:flow_control, DiavasiBench.FlowControl.t()}`,
  `{:heartbeat, DiavasiBench.Heartbeat.t()}`,
  or `{:error, DiavasiBench.ErrorMessage.t()}`.

  This is not `Diavasi.Data.V1.Envelope`.

  ## Examples

      iex> envelope = %DiavasiBench.Envelope{
      ...>   version: 1,
      ...>   body: {:ack, %DiavasiBench.Ack{batch_id: 1}}
      ...> }
      iex> decoded = DiavasiBench.Envelope.decode(DiavasiBench.Envelope.encode(envelope))
      iex> decoded.version
      1
  """
  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field(:version, 1, type: :uint32)

  oneof(:body, 0)

  field(:join_group, 2, type: DiavasiBench.JoinGroup, json_name: "joinGroup", oneof: 0)
  field(:joined, 3, type: DiavasiBench.Joined, oneof: 0)
  field(:record_batch, 4, type: DiavasiBench.RecordBatch, json_name: "recordBatch", oneof: 0)
  field(:ack, 5, type: DiavasiBench.Ack, oneof: 0)
  field(:flow_control, 6, type: DiavasiBench.FlowControl, json_name: "flowControl", oneof: 0)
  field(:heartbeat, 7, type: DiavasiBench.Heartbeat, oneof: 0)
  field(:error, 8, type: DiavasiBench.ErrorMessage, oneof: 0)
end

defmodule DiavasiBench.JoinGroup do
  @moduledoc """
  Bench join request.

  ## Fields

    * `group_id` - `String.t()`.
    * `consumer_id` - `String.t()`.

  ## Examples

      iex> join = %DiavasiBench.JoinGroup{group_id: "bench", consumer_id: "elixir"}
      iex> decoded = DiavasiBench.JoinGroup.decode(DiavasiBench.JoinGroup.encode(join))
      iex> {decoded.group_id, decoded.consumer_id}
      {"bench", "elixir"}
  """
  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field(:group_id, 1, type: :string, json_name: "groupId")
  field(:consumer_id, 2, type: :string, json_name: "consumerId")
end

defmodule DiavasiBench.Joined do
  @moduledoc """
  Bench join reply.

  ## Fields

    * `group_id` - `String.t()`.
    * `consumer_id` - `String.t()`.

  ## Examples

      iex> joined = %DiavasiBench.Joined{group_id: "bench", consumer_id: "elixir"}
      iex> decoded = DiavasiBench.Joined.decode(DiavasiBench.Joined.encode(joined))
      iex> decoded.group_id
      "bench"
  """
  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field(:group_id, 1, type: :string, json_name: "groupId")
  field(:consumer_id, 2, type: :string, json_name: "consumerId")
end

defmodule DiavasiBench.Record do
  @moduledoc """
  One bench record.

  ## Fields

    * `record_id` - `non_neg_integer()`.
    * `payload` - `binary()`.

  ## Examples

      iex> record = %DiavasiBench.Record{record_id: 1, payload: "x"}
      iex> decoded = DiavasiBench.Record.decode(DiavasiBench.Record.encode(record))
      iex> {decoded.record_id, decoded.payload}
      {1, "x"}
  """
  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field(:record_id, 1, type: :uint64, json_name: "recordId")
  field(:payload, 2, type: :bytes)
end

defmodule DiavasiBench.RecordBatch do
  @moduledoc """
  Bench batch.

  ## Fields

    * `batch_id` - `non_neg_integer()`.
    * `records` - list of `DiavasiBench.Record`.
    * `sent_at_unix_ns` - `integer()`. Send time in Unix nanoseconds.

  ## Examples

      iex> batch = %DiavasiBench.RecordBatch{batch_id: 1, records: [], sent_at_unix_ns: 0}
      iex> decoded = DiavasiBench.RecordBatch.decode(DiavasiBench.RecordBatch.encode(batch))
      iex> {decoded.batch_id, decoded.sent_at_unix_ns}
      {1, 0}
  """
  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field(:batch_id, 1, type: :uint64, json_name: "batchId")
  field(:records, 2, repeated: true, type: DiavasiBench.Record)
  field(:sent_at_unix_ns, 3, type: :int64, json_name: "sentAtUnixNs")
end

defmodule DiavasiBench.Ack do
  @moduledoc """
  Bench acknowledgement.

  ## Fields

    * `batch_id` - `non_neg_integer()`.

  ## Examples

      iex> ack = %DiavasiBench.Ack{batch_id: 1}
      iex> decoded = DiavasiBench.Ack.decode(DiavasiBench.Ack.encode(ack))
      iex> decoded.batch_id
      1
  """
  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field(:batch_id, 1, type: :uint64, json_name: "batchId")
end

defmodule DiavasiBench.FlowControl do
  @moduledoc """
  Bench flow-control message.

  ## Fields

    * `max_in_flight` - `non_neg_integer()`.

  ## Examples

      iex> flow = %DiavasiBench.FlowControl{max_in_flight: 2}
      iex> decoded = DiavasiBench.FlowControl.decode(DiavasiBench.FlowControl.encode(flow))
      iex> decoded.max_in_flight
      2
  """
  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field(:max_in_flight, 1, type: :uint32, json_name: "maxInFlight")
end

defmodule DiavasiBench.Heartbeat do
  @moduledoc """
  Empty bench keepalive.

  ## Examples

      iex> beat = %DiavasiBench.Heartbeat{}
      iex> match?(%DiavasiBench.Heartbeat{}, DiavasiBench.Heartbeat.decode(DiavasiBench.Heartbeat.encode(beat)))
      true
  """
  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3
end

defmodule DiavasiBench.ErrorMessage do
  @moduledoc """
  Bench error. `DiavasiBench.TcpClient` raises `message`.

  ## Fields

    * `message` - `String.t()`.

  ## Examples

      iex> err = %DiavasiBench.ErrorMessage{message: "closed"}
      iex> decoded = DiavasiBench.ErrorMessage.decode(DiavasiBench.ErrorMessage.encode(err))
      iex> decoded.message
      "closed"
  """
  use Protobuf, protoc_gen_elixir_version: "0.14.1", syntax: :proto3

  field(:message, 1, type: :string)
end
