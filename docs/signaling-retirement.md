# Redis signaling ownership retirement

M6-04 moves the existing generic best-effort signaling contracts and Redis adapter from component-base v0.7.0 to this SDK. It does not introduce reliable Redis messaging. NSQ remains the supported reliable broker. Hosts retain business payloads, cache reads, polling fallback and authoritative task/report state. No service, database, goroutine or migration is added by the constructor. The host owns its UniversalClient; Watch closes only its PubSub and returns on context cancellation.

The original behavior is preserved. Changes are the package import path, the equivalent non-deprecated Redis Channel(WithChannelSize) call, and removal of an unused private logging helper (the exported ErrorLogger contract remains). Default channel `signal:{SignalName}`, host prefix `qs:signal`, explicit Channel priority, JSON wire, error strings, decode-error continuation, and zero-subscriber publish success remain unchanged. SignalKey remains a host routing hint. ReadTimeout remains reserved and has no new behavior.

The frozen source hashes below refer to component-base main 946328aac16d24748808f5beaed3a41b1271d279 (v0.7.0), not an independently redesigned implementation. Unit/adapter tests and real isolated Redis tests distinguish delivery hints from durability. Supported hosts must compile against a fixed SDK release before component-base removes signaling; runtime upgrades retain prior images and configuration.

```json
{
  "errors.go": "8484a6aced625f68151da32f5bdc31c96282b5dd1f07e2b28e697217332baea2",
  "handler.go": "a589a7cdbd22ceac758b9babe941cff1643f92bc4efc06e9ab7c66ff0b70f151",
  "notifier.go": "1380b4b7147f9b3da1d991e20024c0741f31e8d72654243710e72857fccdac8f",
  "redis/codec.go": "98638cce938426b0005edab4f2a733354b1e7d04b8cafba079bec7becf820541",
  "redis/logger.go": "14eace0ca866d2fbf9c46fdfd6f23d2b9ec012898f5cee2cdd2cc53879ca4499",
  "redis/options.go": "2f79e5bdc76834e95e0daae67b4d97e98063c7c2136f5d90d67c46ead371bce5",
  "redis/signaler.go": "38530e117e8a3052350865adf42f864273435ff7ad2407e89c57ae4d3609a3d5",
  "redis/signaler_test.go": "9dc16791c280d1ea2742954e41aef7c9eb72f8bd2921787b6c88d096a3df9464",
  "signal.go": "27987067ad8e845e68949f7026ed509ea524eadb2d69c6f554378ec4e3084610",
  "watcher.go": "72bed0c4265cb54bf783daae2f45ed8065be7b17e58fe50a5a3be9fdab6ff353"
}
```
