#!/usr/bin/env python3
"""Offline policy/architecture check. Defense in depth, not a credential boundary."""
import pathlib
import re
import subprocess
import sys
import tomllib

MODEL = "gpt-5.6-luna"
ROOT = pathlib.Path(__file__).resolve().parents[1]
GATEWAY = "NO_OPENAI_TRANSPORT_ALLOWED"
EXTENSIONS = {".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs", ".py", ".sh", ".bash", ".yml", ".yaml", ".toml"}
MODEL_PATTERN = re.compile(r"\b(?:gpt-[a-zA-Z0-9.-]+|o[134](?:-mini|-pro)?|chatgpt-[a-zA-Z0-9.-]+|text-embedding-[a-zA-Z0-9.-]+|dall-e-[0-9]+|whisper-[0-9]+)\b")
SDK_IMPORT = re.compile(r"""(?:from\s+['"]openai['"]|(?:import|require)\s*\(\s*['"]openai['"]|^\s*(?:from\s+openai\s+import|import\s+openai\b)|new\s+OpenAI\s*\()""", re.M)
HTTP = re.compile(r"(?:api\.openai\.com|(?:openai|oai)\.azure\.com|azure\.com/openai)", re.I)
DIRECT = re.compile(r"\.(?:responses|completions|embeddings|images|audio|batches)\s*\.\s*(?:create|parse|generate|edit)\s*\(")
def violations(path, source, gateway=GATEWAY):
    errors = []
    # Type-only contracts and schema helpers are not transport.
    runtime = re.sub(r"import\s+type\b[^;]*;", "", source, flags=re.S)
    if path != gateway and (SDK_IMPORT.search(runtime) or HTTP.search(runtime) or DIRECT.search(runtime)):
        errors.append("direct OpenAI transport outside the authorized gateway")
    for match in MODEL_PATTERN.finditer(source):
        if match.group() != MODEL:
            errors.append("non-Luna executable model literal")
            break
    return errors

def self_test():
    bad = [
        'import X from "openai"; new X({});',
        'const x = require("openai");',
        'fetch("https://api.openai.com/v1/responses");',
        'client.responses.create({ model: "gpt-6-astra" });',
        'const model = "gpt-5.6-sol";',
        'const model = "gpt-5.6-terra";',
        'from openai import OpenAI',
        'curl https://api.openai.com/v1/responses',
    ]
    assert all(violations("src/bypass.ts", value, "src/openai.ts") for value in bad)
    assert not violations("src/ok.ts", 'const model = "gpt-5.6-luna";')
    assert not violations("src/openai.ts", 'import OpenAI from "openai";', "src/openai.ts")
    print("policy scanner self-test: PASS")

def verify():
    errors = []
    config = tomllib.loads((ROOT / ".codex/config.toml").read_text())
    a = config.get("agents", {})
    expected = {"enabled": True, "default_subagent_model": MODEL,
                "default_subagent_reasoning_effort": "medium", "max_concurrent_threads_per_session": 4}
    for key, value in expected.items():
        if a.get(key) != value:
            errors.append(f".codex/config.toml: invalid agents.{key}")
    if "model" in config or "max_threads" in a:
        errors.append(".codex/config.toml: parent model pin/legacy thread limit is forbidden")
    for name in ("default", "worker", "explorer"):
        path = ROOT / ".codex/agents" / f"{name}.toml"
        agent = tomllib.loads(path.read_text())
        if agent.get("name") != name or agent.get("model") != MODEL or agent.get("model_reasoning_effort") != "medium" or not agent.get("developer_instructions") or not agent.get("description"):
            errors.append(f"{path.relative_to(ROOT)}: invalid Luna custom agent")
    for path in (ROOT / ".codex/agents").glob("*.toml"):
        if tomllib.loads(path.read_text()).get("model") != MODEL:
            errors.append(f"{path.relative_to(ROOT)}: every custom agent must pin Luna")
    if "## Model and API Cost Safety" not in (ROOT / "AGENTS.md").read_text():
        errors.append("AGENTS.md: missing model/cost policy")
    result = subprocess.run(["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=ROOT, check=True, capture_output=True)
    for name in set(result.stdout.decode().split("\0")):
        path = pathlib.PurePosixPath(name)
        if not name or not (ROOT / name).is_file():
            continue
        if path.name.startswith(".env") and path.name != ".env.example":
            errors.append(f"{name}: environment file must not be tracked")
            continue
        if path.suffix not in EXTENSIONS or name == "scripts/verify_openai_policy.py":
            continue
        if any(part in {"node_modules", ".work", "fixtures", "__tests__", "tests", "test", "docs", "vendor", "dist", "build"} for part in path.parts) or re.search(r"(?:^test_|[.-](?:test|spec)\.)", path.name):
            continue
        source = (ROOT / name).read_text(errors="replace")
        errors.extend(f"{name}: {error}" for error in violations(name, source))
    if errors:
        print("\n".join(errors), file=sys.stderr)
        raise SystemExit(1)
    print("Luna config / custom agents / runtime architecture: PASS (offline; provider lock not verified)")

if __name__ == "__main__":
    if "--self-test" in sys.argv:
        self_test()
    verify()
