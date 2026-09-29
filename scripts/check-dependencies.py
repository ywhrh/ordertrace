#!/usr/bin/env python3
"""Query OSV for runtime Maven coordinates; do not send source code or environment values."""
import json
import pathlib
import re
import subprocess
import xml.etree.ElementTree as ET

root = pathlib.Path(__file__).resolve().parent.parent
subprocess.run(["mvn", "-q", "dependency:list", "-DincludeScope=runtime",
                "-DoutputFile=target/runtime-dependencies.txt"], cwd=root, check=True)
coordinates = re.findall(r"^\s+([^\s:]+):([^\s:]+):jar:([^\s:]+):(compile|runtime)",
                        (root / "target/runtime-dependencies.txt").read_text(), re.M)
queries = [{"package": {"ecosystem": "Maven", "name": f"{g}:{a}"}, "version": v} for g, a, v, _ in coordinates]
response = subprocess.run(["curl", "-fsS", "--max-time", "60", "https://api.osv.dev/v1/querybatch",
                           "-H", "Content-Type: application/json", "--data-binary", "@-"],
                          input=json.dumps({"queries": queries}), text=True, capture_output=True, check=True)
results = json.loads(response.stdout)["results"]
report = {"queries": queries, "results": results}
(root / "target/osv-runtime.json").write_text(json.dumps(report, indent=2))
flagged = 0
for query, result in zip(queries, results):
    if result.get("vulns"):
        flagged += 1
        print(query["package"]["name"], query["version"], [v["id"] for v in result["vulns"]])
print(f"OSV: {flagged} flagged runtime coordinates / {len(queries)} checked. Not a guarantee of vulnerability absence.")

# POM license declarations, including inherited declarations; jar notices still govern redistribution.
ns = {"m": "http://maven.apache.org/POM/4.0.0"}
def licenses(group, artifact, version, seen=None):
    seen = set() if seen is None else seen
    key = (group, artifact, version)
    if key in seen:
        return "UNKNOWN"
    seen.add(key)
    pom = pathlib.Path.home() / ".m2/repository" / group.replace(".", "/") / artifact / version / f"{artifact}-{version}.pom"
    if not pom.exists():
        return "UNKNOWN"
    xml = ET.parse(pom).getroot()
    names = [node.text or "UNKNOWN" for node in xml.findall("m:licenses/m:license/m:name", ns)]
    if names:
        return "; ".join(names)
    parent = xml.find("m:parent", ns)
    if parent is not None:
        return licenses(*(parent.findtext(f"m:{field}", namespaces=ns) for field in ("groupId", "artifactId", "version")), seen)
    return "UNKNOWN"

lines = ["# Runtime dependency license declarations", "", "Generated from resolved Maven POMs. Verify bundled notices before distributing binaries.", "", "| Dependency | Version | Declared license |", "|---|---|---|"]
for group, artifact, version, _ in coordinates:
    lines.append(f"| {group}:{artifact} | {version} | {licenses(group, artifact, version)} |")
(root / "target/dependency-licenses.md").write_text("\n".join(lines) + "\n")
raise SystemExit(1 if flagged else 0)
