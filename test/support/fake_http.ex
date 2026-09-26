defmodule Diavasi.Data.FakeHTTP do
  @moduledoc false

  def start_link(responses, opts \\ []) do
    Agent.start_link(fn ->
      %{
        responses: responses,
        sent: [],
        connect: Keyword.get(opts, :connect, :ok)
      }
    end)
  end

  def connect(_scheme, _host, _port, _opts) do
    agent = agent()

    case Agent.get(agent, & &1.connect) do
      :ok -> {:ok, agent}
      {:error, reason} -> {:error, reason}
    end
  end

  def request(conn, "POST", _path, _headers, :stream), do: {:ok, conn, make_ref()}

  def stream_request_body(agent, _ref, body) do
    Agent.update(agent, fn state -> %{state | sent: [body | state.sent]} end)
    {:ok, agent}
  end

  def recv(agent, _byte_count, _timeout) do
    case Agent.get_and_update(agent, &pop_response/1) do
      :empty -> {:error, agent, :closed, []}
      {:error, reason} -> {:error, agent, reason, []}
      messages -> {:ok, agent, messages}
    end
  end

  def close(_agent), do: :ok

  def sent(agent), do: Agent.get(agent, fn state -> Enum.reverse(state.sent) end)

  defp agent, do: Application.fetch_env!(:diavasi, :http_agent)

  defp pop_response(%{responses: [next | rest]} = state), do: {next, %{state | responses: rest}}
  defp pop_response(state), do: {:empty, state}
end
