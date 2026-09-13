---
title: Connection spine
description: BLE is control only; bulk data goes over Wi-Fi. Order of operations from scan to HTTP media.
---

BLE is control only; bulk data goes over Wi-Fi. Order of operations:

```text
BLE scan → GATT connect → app-pairing → read Wi-Fi creds → join AP → UDP DUML → HTTP media
```

1. **[BLE scan](../ble/).** Identify the camera from manufacturer data or name. No scan filter (Pocket 3 omits manufacturer data).
2. **[GATT](../ble/).** Service `fff0`; notify on `fff4`, write commands to `fff5`. Request MTU 517. Pace writes.
3. **[App-level pairing](../ble/).** Replaces BT bonding. `SetPairingPIN` (`0x07/0x45`); first-time approval is `0x07/0x46`.
4. **[Wi-Fi credentials](../wifi/).** `GetWifiSsid` (`0x07/0x07`) then `GetWifiPassword` (`0x07/0x0e`). Do not synthesize the passphrase.
5. **[Join the AP](../wifi/).** Phone joins the camera SoftAP (WPA2). Camera/gateway is `192.168.2.1`.
6. **[UDP DUML](../duml-transport/).** Port **9004** for the Pocket family, after a TCP `:7001` poke. Xtra rebrands use **10004** with no poke. Then handshake, register, subscribe.
7. **[HTTP media](../media/)** and **[live view](../live-view/)** ride that LAN.

Implementation lives in `Sources/OpenPocketViewCore/`. Platform shells own sockets, permissions, and the Wi-Fi join.

## Node protocol mock

Protocol migration work can run against the dependency-free Node mock in
`tools/opc-mock-camera/` before a physical camera is involved. It implements
the observed UDP `9004`, Pocket TCP `7001`, and HTTP `/v2` surfaces, including
DUML validation, status pushes, media chunks, Range requests, and optional
video packets. Start it with:

```sh
node tools/opc-mock-camera/server.js --host 0.0.0.0 --model pocket4pro
```

The default mock HTTP port is `18080`; use `--http-port 80` when testing a
client that still assumes the camera's port 80. A client on another device
must inject the mock host and ports. The existing physical-camera path is
intentionally tied to `192.168.2.1` and its BLE pairing/Wi-Fi handoff, so the
mock cannot be used by that path without a test-only host injection or a
mock-specific connection adapter. The service does not emulate BLE/GATT or
encode video; real-device checks are still needed for those surfaces.
