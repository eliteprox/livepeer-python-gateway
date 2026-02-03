"""
Example demonstrating single-shot BYOC job execution.

This example shows how to:
1. Connect to an orchestrator and discover BYOC capabilities
2. Execute a single-shot BYOC job (like text-reversal)
3. Handle payments with the remote signer

Usage:
    python byoc_single_shot.py localhost:8935 --capability text-reversal --input "Hello World"
    python byoc_single_shot.py localhost:8935 --capability text-reversal --input "Hello World" --signer http://localhost:8937
"""
import argparse
import json
import sys

from livepeer_gateway import (
    BYOCJobRequest,
    GetOrchestratorInfo,
    LivepeerGatewayError,
    StartBYOCJob,
    get_external_capabilities,
)

DEFAULT_ORCH = "localhost:8935"


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Execute a single-shot BYOC job.")
    p.add_argument(
        "orchestrator",
        nargs="?",
        default=DEFAULT_ORCH,
        help=f"Orchestrator gRPC target (host:port). Default: {DEFAULT_ORCH}",
    )
    p.add_argument(
        "--capability",
        required=True,
        help="Name of the BYOC capability to use (e.g., 'text-reversal', 'comfystream')",
    )
    p.add_argument(
        "--input",
        default="Hello from Python gateway!",
        help="Input data for the BYOC job",
    )
    p.add_argument(
        "--signer",
        default=None,
        help="Remote signer base URL for payments. If omitted, runs in offchain mode.",
    )
    p.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Timeout in seconds for the job (default: 30)",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()

    try:
        print(f"Connecting to orchestrator: {args.orchestrator}")
        print()

        # Get orchestrator info
        info = GetOrchestratorInfo(args.orchestrator, signer_url=args.signer)
        print(f"Orchestrator: {info.transcoder}")
        print(f"ETH Address: {info.address.hex()}")
        print()

        # List external capabilities
        ext_caps = get_external_capabilities(info)
        if not ext_caps:
            print("No external/BYOC capabilities available on this orchestrator.")
            print("Make sure the orchestrator has BYOC capabilities registered.")
            sys.exit(1)

        print("Available BYOC capabilities:")
        for cap in ext_caps:
            available = cap.capacity_available
            print(f"  - {cap.name}: capacity={cap.capacity}, in_use={cap.capacity_in_use}, available={available}")
            if cap.description:
                print(f"    Description: {cap.description}")
            if cap.price_per_unit > 0:
                print(f"    Price: {cap.price_per_unit} wei per {cap.price_scaling} unit(s)")
        print()

        # Check if requested capability exists
        cap_names = [c.name for c in ext_caps]
        if args.capability not in cap_names:
            print(f"Error: Capability '{args.capability}' not found on orchestrator.")
            print(f"Available capabilities: {', '.join(cap_names)}")
            sys.exit(1)

        # Build and execute the BYOC job
        print(f"Executing BYOC job: capability={args.capability}")
        print(f"Input: {args.input}")
        print()

        # Build the request data based on capability type
        # For text-reversal, the request is just the text
        request_data = {"text": args.input}

        req = BYOCJobRequest(
            capability=args.capability,
            request=request_data,
            timeout_seconds=args.timeout,
        )

        response = StartBYOCJob(
            info,
            req,
            signer_base_url=args.signer,
        )

        print("Job completed successfully!")
        print(f"Response: {json.dumps(response.raw, indent=2)}")
        if response.balance is not None:
            print(f"Remaining balance: {response.balance}")

    except LivepeerGatewayError as e:
        print(f"Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
