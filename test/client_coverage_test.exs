defmodule Diavasi.Data.ClientCoverageTest do
  use ExUnit.Case, async: false

  alias Diavasi.Data.Client
  alias Diavasi.Data.FakeHTTP
  alias Mix.Tasks.Diavasi.Consume

  alias Diavasi.Data.V1.{
    Ack,
    Envelope,
    ErrorMessage,
    Heartbeat,
    HelloAck,
    Joined,
    Nack,
    Record,
    RecordBatch
  }

  setup do
    previous = Application.get_env(:diavasi, :http_client)
    Application.put_env(:diavasi, :http_client, FakeHTTP)

    on_exit(fn ->
      restore(:http_client, previous)
      Application.delete_env(:diavasi, :http_agent)
    end)

    :ok
  end

  test "run acks each batch and leaves" do
    agent =
      script([
        [data(hello_ack())],
        [status(200), data(joined()), data(batch(1, [1])), data(batch(2, [2, 3]))]
      ])

    assert {:ok, [1, 2, 3], [1, 2]} = Client.run(opts(total: 3))

    assert sent_bodies(agent) == [
             :hello,
             :join_group,
             :flow_control,
             {:ack, 1},
             {:ack, 2},
             :leave,
             :eof
           ]
  end

  test "halt_after disconnects without Leave" do
    agent =
      script([
        [data(hello_ack())],
        [data(joined()), data(batch(7, [4]))],
        [data(heartbeat()), data(batch(8, [5]))]
      ])

    assert {:ok, [4], [7]} = Client.run(opts(total: 8, halt_after: 1))
    assert :leave not in sent_bodies(agent)
    assert {:ack, 7} in sent_bodies(agent)
    assert :heartbeat in sent_bodies(agent)
  end

  test "stream yields until the session is done" do
    _agent =
      script([
        [data(hello_ack())],
        [headers([{"grpc-status", "0"}]), data(joined()), data(batch(1, [9]))],
        [{:done, make_ref()}]
      ])

    assert {:ok, pid} = Client.start_link(opts())
    assert [%{batch_id: 1}] = pid |> Client.stream() |> Enum.to_list()
  end

  test "stream raises a protocol error" do
    _agent =
      script([
        [data(hello_ack())],
        [data(joined())],
        [data(error_frame(5, "not running"))]
      ])

    assert {:ok, pid} = Client.start_link(opts())

    assert_raise RuntimeError, "protocol error 5: not running", fn ->
      pid |> Client.stream() |> Enum.to_list()
    end
  end

  test "handshake reports protocol, grpc, status, close, and unexpected frames" do
    assert {:error, "protocol error 5: not running"} =
             Client.run(fail_after([data(error_frame(5, "not running"))], total: 1))

    assert {:error, "grpc 16: not authorized"} =
             Client.run(
               fail_after(
                 [
                   {:headers, make_ref(),
                    [{"grpc-status", "16"}, {"grpc-message", "not%20authorized"}]}
                 ],
                 total: 1
               )
             )

    assert {:error, "grpc 401: "} = Client.run(fail_after([status(401)], total: 1))
    assert {:error, "stream closed"} = Client.run(fail_after([{:done, make_ref()}], total: 1))

    assert {:error, "unexpected frame " <> _} =
             Client.run(
               fail_after([data(%Envelope{version: 1, body: {:nack, %Nack{batch_id: 1}}})],
                 total: 1
               )
             )
  end

  test "a refused connection and an http2 failure are errors" do
    script([], connect: {:error, :econnrefused})
    assert {:error, ":econnrefused"} = Client.run(opts(total: 1))

    script([
      [data(hello_ack())],
      {:error, :closed}
    ])

    assert {:error, "http2 error :closed"} = Client.run(opts(total: 1))
  end

  test "a split frame is assembled before the join" do
    full = frame(hello_ack())
    <<head::binary-size(3), tail::binary>> = full

    script([
      [{:data, make_ref(), head}],
      [{:headers, make_ref(), [{"content-type", "application/grpc"}]}, {:data, make_ref(), tail}],
      [data(joined()), data(batch(1, [1]))]
    ])

    assert {:ok, [1], [1]} = Client.run(opts(total: 1))
  end

  test "a heartbeat during the handshake and a trailing done close the stream" do
    script([
      [data(heartbeat()), data(hello_ack())],
      [
        data(joined()),
        data(%Envelope{version: 1, body: {:nack, %Nack{batch_id: 9}}}),
        data(batch(1, [1])),
        {:done, make_ref()}
      ]
    ])

    assert {:ok, pid} = Client.start_link(opts())
    assert [%{batch_id: 1}] = pid |> Client.stream() |> Enum.to_list()
    assert :ok = Client.ack(pid, 1)
    assert :ok = Client.leave(pid)
  end

  test "a grpc trailer without a message fails the handshake" do
    assert {:error, "grpc 14: "} =
             Client.run(fail_after([headers([{"grpc-status", "14"}])], total: 1))
  end

  test "a heartbeat alone is followed by another read" do
    script([
      [data(hello_ack())],
      [data(joined())],
      [data(heartbeat())],
      [data(batch(1, [6]))]
    ])

    assert {:ok, [6], [1]} = Client.run(opts(total: 1))
  end

  test "errors after Joined are returned from run" do
    script([
      [data(hello_ack())],
      [data(joined()), data(error_frame(4, "bad batch"))]
    ])

    assert {:error, "protocol error 4: bad batch"} = Client.run(opts(total: 1))

    script([
      [data(hello_ack())],
      [data(joined())],
      {:error, :closed}
    ])

    assert {:error, "http2 error :closed"} = Client.run(opts(total: 1))

    script([
      [data(hello_ack())],
      [data(joined())],
      [headers([{"grpc-status", "13"}, {"grpc-message", "internal"}])]
    ])

    assert {:error, "grpc 13: internal"} = Client.run(opts(total: 1))
  end

  test "an empty session is an incomplete consume" do
    assert {:error, "incomplete consume records=0 batches=0"} =
             Client.run(
               fail_after([{:done, make_ref()}],
                 hello: true,
                 joined: true,
                 total: 1
               )
             )
  end

  test "disconnect is a no-op after the process has stopped" do
    script([
      [data(hello_ack())],
      [data(joined()), data(batch(1, [1]))]
    ])

    assert {:ok, pid} = Client.start_link(opts())
    assert :ok = Client.disconnect(pid)
    assert :ok = Client.disconnect(pid)
  end

  test "the consume task prints ids and exits on a protocol error" do
    script([
      [data(hello_ack())],
      [data(joined()), data(batch(3, [8]))]
    ])

    output =
      ExUnit.CaptureIO.capture_io(fn ->
        Consume.run(~w(--addr 127.0.0.1:1 --ca ca.pem --token t --group g --total 1))
      end)

    assert output =~ "record_ids 8"
    assert output =~ "batch_ids 3"

    script([
      [data(hello_ack())],
      [data(error_frame(5, "not running"))]
    ])

    assert catch_exit(
             ExUnit.CaptureIO.capture_io(:stderr, fn ->
               Consume.run(~w(--addr 127.0.0.1:1 --ca ca.pem --token t --group g --total 1))
             end)
           ) == {:shutdown, 5}

    script([], connect: {:error, :econnrefused})

    assert catch_exit(
             ExUnit.CaptureIO.capture_io(:stderr, fn ->
               Consume.run(~w(--addr 127.0.0.1:1 --ca ca.pem --token t --group g --total 1))
             end)
           ) == {:shutdown, 1}
  end

  defp fail_after(messages, opts) do
    {hello, opts} = Keyword.pop(opts, :hello, true)
    {joined, opts} = Keyword.pop(opts, :joined, false)

    steps =
      []
      |> then(fn steps -> if hello, do: steps ++ [[data(hello_ack())]], else: steps end)
      |> then(fn steps -> if joined, do: steps ++ [[data(joined())]], else: steps end)
      |> Kernel.++([messages])

    script(steps)
    opts(opts)
  end

  defp script(responses, script_opts \\ []) do
    {:ok, agent} = FakeHTTP.start_link(responses, script_opts)
    Application.put_env(:diavasi, :http_agent, agent)
    agent
  end

  defp opts(extra \\ []),
    do: [addr: "127.0.0.1:1", ca: "ca.pem", token: "t", group: "demo"] ++ extra

  defp sent_bodies(agent) do
    Enum.flat_map(FakeHTTP.sent(agent), &body_names/1)
  end

  defp body_names(:eof), do: [:eof]

  defp body_names(<<0, len::32, payload::binary-size(len), rest::binary>>) do
    [body_name(Envelope.decode(payload)) | body_names(rest)]
  end

  defp body_names(<<>>), do: []

  defp body_name(%Envelope{body: {:hello, _}}), do: :hello
  defp body_name(%Envelope{body: {:join_group, _}}), do: :join_group
  defp body_name(%Envelope{body: {:flow_control, _}}), do: :flow_control
  defp body_name(%Envelope{body: {:ack, %Ack{batch_id: id}}}), do: {:ack, id}
  defp body_name(%Envelope{body: {:leave, _}}), do: :leave
  defp body_name(%Envelope{body: {:heartbeat, _}}), do: :heartbeat

  defp data(envelope), do: {:data, make_ref(), frame(envelope)}
  defp status(code), do: {:status, make_ref(), code}
  defp headers(list), do: {:headers, make_ref(), list}

  defp frame(envelope) do
    bin = Envelope.encode(envelope)
    <<0, byte_size(bin)::32, bin::binary>>
  end

  defp hello_ack, do: %Envelope{version: 1, body: {:hello_ack, %HelloAck{protocol_version: 1}}}

  defp joined,
    do: %Envelope{version: 1, body: {:joined, %Joined{group_id: "demo", consumer_id: "elixir"}}}

  defp batch(id, record_ids) do
    records = Enum.map(record_ids, &%Record{record_id: &1, payload: "x"})
    %Envelope{version: 1, body: {:record_batch, %RecordBatch{batch_id: id, records: records}}}
  end

  defp error_frame(code, message) do
    %Envelope{version: 1, body: {:error, %ErrorMessage{code: code, message: message}}}
  end

  defp heartbeat, do: %Envelope{version: 1, body: {:heartbeat, %Heartbeat{}}}

  defp restore(key, nil), do: Application.delete_env(:diavasi, key)
  defp restore(key, value), do: Application.put_env(:diavasi, key, value)
end
