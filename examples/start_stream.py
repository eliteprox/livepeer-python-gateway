"""
Start a BYOC streaming job and display the response (analogous to start_job.py
for LV2V jobs).

This example shows how to:
1. Connect to an orchestrator and verify a BYOC capability
2. Start a streaming job with optional workflow/params
3. Display the stream URLs and metadata

Usage:
    # Basic: start a comfystream session with default params
    python start_stream.py localhost:8935 --capability comfystream

    # With a ComfyUI API-format workflow file
    python start_stream.py localhost:8935 --capability comfystream \
        --workflow workflow-api.json

    # With explicit params (overrides/merges with workflow)
    python start_stream.py localhost:8935 --capability comfystream \
        --workflow workflow-api.json --params '{"width": 768, "height": 768}'

    # With payments via remote signer
    python start_stream.py localhost:8935 --capability comfystream \
        --signer http://localhost:8937 --workflow workflow-api.json
"""

import argparse
import json

from livepeer_gateway import (
    BYOCStreamRequest,
    GetBYOCJobToken,
    GetOrchestratorInfo,
    LivepeerGatewayError,
    StartBYOCStreamWithRetry,
    fetch_external_capabilities,
)

DEFAULT_ORCH = "localhost:8935"


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Start a BYOC streaming job and display the response."
    )
    p.add_argument(
        "orchestrator",
        nargs="?",
        default=DEFAULT_ORCH,
        help=f"Orchestrator gRPC target (host:port). Default: {DEFAULT_ORCH}",
    )
    p.add_argument(
        "--capability",
        default="comfystream",
        help="BYOC capability name (default: comfystream).",
    )
    p.add_argument(
        "--signer",
        default=None,
        help="Remote signer base URL (no path). If omitted, runs in offchain mode.",
    )
    p.add_argument(
        "--workflow",
        default=None,
        metavar="JSON_FILE",
        help="Path to a ComfyUI API-format workflow JSON file. "
        "The workflow is sent as the 'prompts' key in stream params.",
    )
    p.add_argument(
        "--params",
        default=None,
        metavar="JSON",
        help="Stream params as a JSON string. Merged with workflow if both provided. "
        "Example: '{\"prompts\": {...}, \"width\": 512, \"height\": 512}'",
    )
    p.add_argument(
        "--no-video-ingress",
        action="store_true",
        help="Disable video input channel.",
    )
    p.add_argument(
        "--no-video-egress",
        action="store_true",
        help="Disable video output channel.",
    )
    p.add_argument(
        "--enable-data-output",
        action="store_true",
        help="Enable data output channel.",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    orch_url = args.orchestrator

    try:
        # --- Connect to orchestrator ---
        info = GetOrchestratorInfo(orch_url, signer_url=args.signer)
        print("=== OrchestratorInfo ===")
        print("Orchestrator:", orch_url)
        print("Transcoder URI:", info.transcoder)
        print("ETH Address:", info.address.hex())
        print()

        # --- Verify capability ---
        ext_caps = fetch_external_capabilities(orch_url)
        cap_names = [c.name for c in ext_caps]
        if args.capability not in cap_names:
            available = ", ".join(cap_names) if cap_names else "none"
            raise LivepeerGatewayError(
                f"Capability '{args.capability}' not found. Available: {available}"
            )

        cap_info = next(c for c in ext_caps if c.name == args.capability)
        print(f"Capability: {cap_info.name}")
        print(f"  description: {cap_info.description or 'N/A'}")
        print(f"  capacity: {cap_info.capacity} (available: {cap_info.capacity_available})")

        if args.signer:
            try:
                job_token = GetBYOCJobToken(info, args.capability, args.signer)
                print(f"  price: {job_token.price_per_unit} wei per {job_token.pixels_per_unit} unit(s)")
                print(f"  balance: {job_token.balance}")
            except Exception as e:
                print(f"  price: (could not fetch job token: {e})")
        print()

        # --- Build stream params ---
        stream_params: dict = {}

        # Load workflow from JSON file if provided
        if args.workflow:
            with open(args.workflow) as f:
                workflow = json.load(f)
            stream_params["prompts"] = workflow
            print(f"Workflow: {args.workflow} ({len(workflow)} nodes)")

        # Merge any additional params
        if args.params:
            extra = json.loads(args.params)
            stream_params.update(extra)

        if stream_params:
            print("Stream params:", json.dumps(
                {k: f"<{len(v)} nodes>" if k == "prompts" and isinstance(v, dict) else v
                 for k, v in stream_params.items()},
                indent=2,
            ))
            print()

        # --- Start the stream ---
        stream_req = BYOCStreamRequest(
            capability=args.capability,
            params=stream_params,
            enable_video_ingress=not args.no_video_ingress,
            enable_video_egress=not args.no_video_egress,
            enable_data_output=args.enable_data_output,
        )

        stream_job = StartBYOCStreamWithRetry(
            info,
            stream_req,
            signer_base_url=args.signer,
            max_retries=2,
        )

        print("=== BYOC Stream Started ===")
        print("stream_id    :", stream_job.stream_id)
        print("capability   :", stream_job.capability)
        print("publish_url  :", stream_job.publish_url or "N/A")
        print("subscribe_url:", stream_job.subscribe_url or "N/A")
        print("control_url  :", stream_job.control_url or "N/A")
        print("events_url   :", stream_job.events_url or "N/A")
        print("data_url     :", stream_job.data_url or "N/A")
        print("balance      :", stream_job.balance)
        print()
        print(json.dumps(stream_job.raw, indent=2, sort_keys=True))
        print()

    except LivepeerGatewayError as e:
        print(f"ERROR ({orch_url}): {e}")
        print()


if __name__ == "__main__":
    main()
