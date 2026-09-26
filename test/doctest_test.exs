defmodule Diavasi.DoctestTest do
  use ExUnit.Case, async: false

  alias Diavasi.Data.Client
  alias Diavasi.Data.HTTP

  setup do
    on_exit(fn ->
      Client.reset_client()
      HTTP.reset_client()
    end)

    :ok
  end

  doctest Diavasi.Data.Client
  doctest Diavasi.Data.Client.Behaviour
  doctest Diavasi.Data.HTTP
  doctest Diavasi.Data.V1.Hello
  doctest Diavasi.Data.V1.HelloAck
  doctest Diavasi.Data.V1.JoinGroup
  doctest Diavasi.Data.V1.Joined
  doctest Diavasi.Data.V1.Record
  doctest Diavasi.Data.V1.RecordBatch
  doctest Diavasi.Data.V1.Ack
  doctest Diavasi.Data.V1.Nack
  doctest Diavasi.Data.V1.Heartbeat
  doctest Diavasi.Data.V1.FlowControl
  doctest Diavasi.Data.V1.ErrorMessage
  doctest Diavasi.Data.V1.Leave
  doctest Diavasi.Data.V1.Envelope
  doctest Mix.Tasks.Diavasi.Consume
  doctest DiavasiBench.TcpClient
  doctest Mix.Tasks.Diavasi.Bench
  doctest DiavasiBench.Envelope
  doctest DiavasiBench.JoinGroup
  doctest DiavasiBench.Joined
  doctest DiavasiBench.Record
  doctest DiavasiBench.RecordBatch
  doctest DiavasiBench.Ack
  doctest DiavasiBench.FlowControl
  doctest DiavasiBench.Heartbeat
  doctest DiavasiBench.ErrorMessage
end
