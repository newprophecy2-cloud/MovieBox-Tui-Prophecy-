#!/usr/bin/env python3
"""Verify live FourKHDHub resolver chains without printing signed URLs.

Usage:
    python3 scripts/live_resolver_test.py
    python3 scripts/live_resolver_test.py --base-url https://example.test

The script intentionally uses /playback because that endpoint represents explicit
playback intent and invokes the provider resolver. It reports only safe metadata.
"""

from __future__ import annotations

import argparse
import base64
import json
import shutil
import subprocess
import sys
import urllib.parse
import urllib.request
from dataclasses import dataclass


DEFAULT_BASE_URL = "https://moviebox-tui-prophecy.onrender.com"


@dataclass(frozen=True)
class Case:
    title: str
    provider: str
    subject_id: str
    season: int = 1
    episode: int = 7


CASES = (
    Case("Dan Da Dan", "fourkhdhub", "/dan-da-dan-series-111/"),
    Case("Lookism", "fourkhdhub", "/lookism-series-1432/"),
    Case("Blue Lock", "fourkhdhub", "/blue-lock-series-135/"),
)


def opaque_id(provider: str, subject_id: str, season: int, episode: int) -> str:
    payload = {
        "p": provider,
        "v": subject_id,
        "s": season,
        "e": episode,
    }
    raw = json.dumps(payload, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def get_json(base_url: str, path: str, timeout: float) -> tuple[int, dict]:
    request = urllib.request.Request(
        base_url.rstrip("/") + path,
        headers={"Accept": "application/json", "User-Agent": "MovieBox-live-resolver-test/1"},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = response.read()
        return response.status, json.loads(body)


def probe_with_ffprobe(url: str, timeout: float) -> tuple[bool, str]:
    if shutil.which("ffprobe") is None:
        return False, "ffprobe-not-installed"
    command = [
        "ffprobe",
        "-v",
        "error",
        "-rw_timeout",
        str(int(timeout * 1_000_000)),
        "-i",
        url,
        "-show_entries",
        "format=format_name,duration:stream=codec_name,codec_type,width,height",
        "-of",
        "default=noprint_wrappers=1:nokey=0",
    ]
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=timeout + 5,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return False, "ffprobe-timeout"
    if result.returncode != 0:
        return False, "ffprobe-invalid-media"
    fields = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    return bool(fields), "; ".join(fields[:12]) or "no-media-metadata"


def run_case(base_url: str, case: Case, timeout: float) -> bool:
    episode_id = opaque_id(case.provider, case.subject_id, case.season, case.episode)
    encoded_episode = urllib.parse.quote(episode_id, safe="")
    try:
        source_status, source_response = get_json(
            base_url,
            f"/api/v1/episodes/{encoded_episode}/sources",
            timeout,
        )
    except Exception as error:  # noqa: BLE001 - CLI should report per-case failure.
        print(f"FAIL {case.title}: source request failed ({type(error).__name__})")
        return False

    sources = source_response.get("sources", [])
    if source_status != 200 or not sources:
        print(f"FAIL {case.title}: source_status={source_status} source_count={len(sources)}")
        return False

    source = sources[0]
    source_id = source.get("id")
    if not isinstance(source_id, str) or not source_id:
        print(f"FAIL {case.title}: source ID missing")
        return False

    encoded_source = urllib.parse.quote(source_id, safe="")
    try:
        playback_status, playback = get_json(
            base_url,
            f"/api/v1/sources/{encoded_source}/playback",
            timeout,
        )
    except Exception as error:  # noqa: BLE001 - CLI should report per-case failure.
        print(f"FAIL {case.title}: playback request failed ({type(error).__name__})")
        return False

    url = playback.get("url")
    if playback_status != 200 or playback.get("resolved") is not True or not isinstance(url, str):
        code = playback.get("error", {}).get("code") if isinstance(playback.get("error"), dict) else None
        print(f"FAIL {case.title}: playback_status={playback_status} resolved={playback.get('resolved')} error={code or 'none'}")
        return False

    host = urllib.parse.urlparse(url).hostname or ""
    intermediate_hosts = ("greenmotors.", "greenmountmotors.", "hubcloud.", "hubdrive.")
    if any(marker in host.lower() for marker in intermediate_hosts):
        print(f"FAIL {case.title}: resolver returned intermediate host={host}")
        return False

    playable, media = probe_with_ffprobe(url, timeout)
    if not playable:
        print(f"FAIL {case.title}: final URL is not probeable ({media})")
        return False

    print(
        f"PASS {case.title}: source_count={len(sources)} "
        f"label={source.get('label', 'unknown')} quality={source.get('quality', 'unknown')} "
        f"final_host={host} media={media}"
    )
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--timeout", type=float, default=90.0)
    args = parser.parse_args()

    print(f"Live resolver target: {args.base_url.rstrip('/')}")
    results = [run_case(args.base_url, case, args.timeout) for case in CASES]
    passed = sum(results)
    print(f"Summary: {passed}/{len(results)} resolver cases passed")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
