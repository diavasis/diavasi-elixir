# Changelog

## 0.1.0

First release of the Hex package `diavasi`.

* `Diavasi.Data.Client.put_client/1` installs a `Diavasi.Data.Client.Behaviour` implementation, including a Mox mock, in place of the real client.
* `Diavasi.Data.HTTP.put_client/1` replaces `Mint.HTTP` for tests that still run the real client.
