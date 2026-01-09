from livepeer_gateway.orchestrator import GetOrchestratorInfo, LivepeerGatewayError

ORCH_URL = "localhost:8935"
SIGNER_URL = "https://vyt5g5r8tu9hrv.transfix.ai/sign-orchestrator-info"  # adjust


def main() -> None:
    try:

        info = GetOrchestratorInfo(ORCH_URL, signer_url=SIGNER_URL)

        print("=== OrchestratorInfo ===")
        print("Transcoder URI:", info.transcoder)
        print("ETH Address:", info.address.hex())

        # Call again to demonstrate signer caching (no second signer HTTP request)
        info2 = GetOrchestratorInfo(ORCH_URL, signer_url=SIGNER_URL)

        print("Second call OK; same signer material cached.")

    except LivepeerGatewayError as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    main()
