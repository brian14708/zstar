"""Adapt Griffe's static dump to starlight-pydocs' member and base handling."""

import json
import subprocess
import sys
from pathlib import Path


def prepare_members(obj: dict) -> None:
    if obj.get("kind") == "class":
        # The plugin cannot resolve parameterized bases for inherited members.
        # Internal mixin type arguments are irrelevant to the public reference.
        obj["bases"] = [
            base["left"]
            if isinstance(base, dict)
            and base.get("cls") == "ExprSubscript"
            and base["left"].get("name", "").startswith("_")
            else base
            for base in obj.get("bases", [])
        ]
    for member in obj.get("members", {}).values():
        prepare_members(member)


def main() -> None:
    args = sys.argv[1:]
    subprocess.run([sys.executable, "-m", "griffe", *args], check=True)
    output = Path(args[args.index("-o") + 1])
    dump = json.loads(output.read_text())
    for package in dump.values():
        prepare_members(package)
    output.write_text(json.dumps(dump))


if __name__ == "__main__":
    main()
