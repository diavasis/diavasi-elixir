Mox.defmock(Diavasi.Data.HTTP.Mock, for: Diavasi.Data.HTTP)
Mox.defmock(Diavasi.Data.Client.Mock, for: Diavasi.Data.Client.Behaviour)

ExUnit.start(exclude: [:integration])
