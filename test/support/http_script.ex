defmodule Diavasi.Data.HTTPScript do
  @moduledoc false

  import Mox

  alias Diavasi.Data.HTTP
  alias Diavasi.Data.HTTP.Mock

  def install(responses, opts \\ []) do
    {:ok, agent} =
      Agent.start_link(fn ->
        %{
          responses: responses,
          sent: [],
          connect: Keyword.get(opts, :connect, :ok)
        }
      end)

    stub(Mock, :connect, fn _scheme, _host, _port, _opts ->
      case Agent.get(agent, & &1.connect) do
        :ok -> {:ok, agent}
        {:error, reason} -> {:error, reason}
      end
    end)

    stub(Mock, :request, fn conn, "POST", _path, _headers, :stream ->
      {:ok, conn, make_ref()}
    end)

    stub(Mock, :stream_request_body, fn conn, _ref, body ->
      Agent.update(agent, fn state -> %{state | sent: [body | state.sent]} end)
      {:ok, conn}
    end)

    stub(Mock, :recv, fn conn, _byte_count, _timeout ->
      case Agent.get_and_update(agent, &pop_response/1) do
        :empty -> {:error, conn, :closed, []}
        {:error, reason} -> {:error, conn, reason, []}
        messages -> {:ok, conn, messages}
      end
    end)

    stub(Mock, :close, fn _conn -> :ok end)

    HTTP.put_client(Mock)
    agent
  end

  def sent(agent), do: Agent.get(agent, fn state -> Enum.reverse(state.sent) end)

  defp pop_response(%{responses: [next | rest]} = state), do: {next, %{state | responses: rest}}
  defp pop_response(state), do: {:empty, state}
end
