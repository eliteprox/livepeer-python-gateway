# Remote signer payment envelopes (PymtHouse)

PymtHouse routes `POST /api/v1/signer/generate-live-payment` **per request** based on the JSON body. The python-gateway SDK exposes two session types that map to those bodies.

## `PaymentSession` (legacy orchestrator path)

Use when you have a negotiated **orchestrator** (base64 `OrchestratorInfo` on the wire).

- Body includes `orchestrator` (or `Orchestrator`) plus pipeline / manifest / pricing fields.
- PymtHouse forwards to **go-livepeer** remote signer when the app is `legacy_remote_signer`, or to LPNM orchestrator signing when the app is `lpnm_payer_daemon`.
- In **`dual`** app mode, an orchestrator blob is routed to the **legacy** remote signer.

```python
from livepeer_gateway.remote_signer import PaymentSession

session = PaymentSession(signer_url, orchestrator_info_b64, ...)
ticket = await session.get_payment()
```

## `RegistryPaymentSession` (registry / LPNM path)

Use for **registry-backed** capabilities without legacy `OrchestratorInfo`.

- Body includes `paymentMode: "registry"`, `capability`, `offering`, `recipient`, `ticketParamsBaseUrl`, and pricing fields.
- PymtHouse uses the **LPNM payer-daemon** when the app enables LPNM (`lpnm_payer_daemon` or `dual`).
- Legacy-only apps return **403** for registry bodies.

```python
from livepeer_gateway.registry_payment_session import RegistryPaymentSession
from livepeer_gateway.registry_types import RegistryRouteCandidate

candidate = RegistryRouteCandidate(...)
session = RegistryPaymentSession(signer_url, candidate, pipeline="openai:audio-speech")
ticket = await session.get_payment()
```

## App `signing_mode` on PymtHouse

| App mode | Registry body | Orchestrator blob |
|----------|---------------|-------------------|
| `legacy_remote_signer` | 403 | Legacy DMZ |
| `lpnm_payer_daemon` | LPNM registry | LPNM orchestrator |
| `dual` | LPNM registry | Legacy DMZ |

Configure via `PUT /api/v1/apps/:id` with `signingMode`. See PymtHouse `docs/LPNM_SIGNING.md`.

## Discovery and aux routes

`sign-orchestrator-info`, `sign-byoc-job`, and `discover-orchestrators` follow the app’s **aux** route: LPNM-only apps use the payer daemon; dual and legacy apps use the legacy remote signer unless only LPNM is enabled.
