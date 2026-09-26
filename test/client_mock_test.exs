defmodule Diavasi.Data.ClientMockTest do
  use ExUnit.Case, async: false

  import Mox

  alias Diavasi.Data.Client
  alias Diavasi.Data.Client.Mock

  setup :verify_on_exit!

  setup do
    previous = Application.get_env(:diavasi, :client)
    Client.put_client(Mock)

    on_exit(fn ->
      Client.reset_client()

      if previous do
        Application.put_env(:diavasi, :client, previous)
      end
    end)

    :ok
  end

  test "put_client forwards calls to a Mox mock" do
    pid = self()

    expect(Mock, :start_link, fn opts ->
      assert opts[:group] == "demo"
      {:ok, pid}
    end)

    assert {:ok, ^pid} = Client.start_link(group: "demo")

    expect(Mock, :next_batch, fn ^pid -> {:ok, %{batch_id: 1}} end)
    assert {:ok, %{batch_id: 1}} = Client.next_batch(pid)

    expect(Mock, :ack, fn ^pid, 1 -> :ok end)
    assert :ok = Client.ack(pid, 1)

    expect(Mock, :leave, fn ^pid -> :ok end)
    assert :ok = Client.leave(pid)

    expect(Mock, :disconnect, fn ^pid -> :ok end)
    assert :ok = Client.disconnect(pid)

    expect(Mock, :stream, fn ^pid -> [%{batch_id: 1}] end)
    assert [%{batch_id: 1}] = Client.stream(pid)

    expect(Mock, :run, fn opts ->
      assert opts[:total] == 1
      {:ok, [1], [1]}
    end)

    assert {:ok, [1], [1]} = Client.run(total: 1)
    assert :ok = Client.reset_client()
    assert Application.get_env(:diavasi, :client) == nil
  end
end
