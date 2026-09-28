Mox.defmock(Diavasi.HTTP.Mock, for: Diavasi.HTTP)
Mox.defmock(Diavasi.Client.Mock, for: Diavasi.Client.Behaviour)

ExUnit.start(exclude: [:integration])
