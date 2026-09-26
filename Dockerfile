FROM elixir:1.17
WORKDIR /app
COPY . /app
RUN mix local.hex --force && mix deps.get
RUN chmod +x /app/wait-ca.sh
CMD ["/bin/sh", "-c", "/app/wait-ca.sh && mix diavasi.consume --addr \"$DIAVASI_DATA_ADDR\" --ca \"$DIAVASI_CA\" --token \"$DIAVASI_API_TOKEN\" --group demo --consumer elixir --total 8"]
