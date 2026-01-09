import argparse

from livepeer_gateway.orchestrator import GetOrchestratorInfo, LivepeerGatewayError

DEFAULT_ORCH = "localhost:8935"
DEFAULT_SIGNER_URL = "https://vyt5g5r8tu9hrv.transfix.ai"  # adjust


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fetch orchestrator info via Livepeer gRPC.")
    p.add_argument(
        "orchestrators",
        nargs="*",
        default=[DEFAULT_ORCH],
        help=f"One or more orchestrator gRPC targets (host:port). Default: {DEFAULT_ORCH}",
    )
    p.add_argument(
        "--signer-url",
        default=DEFAULT_SIGNER_URL,
        help="Remote signer base URL (no path). Used for /sign-orchestrator-info.",
    )
    return p.parse_args()

def main() -> None:
    args = _parse_args()

    for orch_url in args.orchestrators:
        try:
            info = GetOrchestratorInfo(orch_url, signer_url=args.signer_url)

            print("=== OrchestratorInfo ===")
            print("Orchestrator:", orch_url)
            print("Transcoder URI:", info.transcoder)
            print("ETH Address:", info.address.hex())
            print()

        except LivepeerGatewayError as e:
            print(f"ERROR ({orch_url}): {e}")
            print()

if __name__ == "__main__":
    main()
