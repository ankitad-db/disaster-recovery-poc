"""Generate docs/architecture_simple.excalidraw — an exec-level (~7 box) view.

Run: python docs/_gen_excalidraw_simple.py
The detailed diagram lives in architecture.excalidraw; this is the one-glance story.
"""
import json
import os
import random

els = []
_i = [0]


def idx():
    _i[0] += 1
    return f"a{_i[0]:03d}"


def _base(x, y, w, h, stroke, bg, **extra):
    e = {
        "x": x, "y": y, "width": w, "height": h, "angle": 0,
        "strokeColor": stroke, "backgroundColor": bg, "fillStyle": "solid",
        "strokeWidth": 2, "strokeStyle": "solid", "roughness": 0, "opacity": 100,
        "groupIds": [], "frameId": None, "index": idx(),
        "roundness": {"type": 3}, "seed": random.randint(1, 2**31),
        "version": 1, "versionNonce": random.randint(1, 2**31),
        "isDeleted": False, "boundElements": [], "updated": 1779000000000,
        "link": None, "locked": False,
    }
    e.update(extra)
    return e


def rect(x, y, w, h, stroke="#1e1e1e", bg="transparent"):
    e = _base(x, y, w, h, stroke, bg, type="rectangle")
    e["id"] = f"r{_i[0]}"
    els.append(e)
    return e["id"]


def text(x, y, s, size=16, color="#1e1e1e", bold=False, align="left"):
    lines = s.split("\n")
    e = _base(x, y, max(len(l) for l in lines) * size * 0.6, len(lines) * size * 1.25,
              color, "transparent", type="text")
    e.update({"id": f"t{_i[0]}", "text": s, "fontSize": size, "fontFamily": 5 if bold else 2,
              "textAlign": align, "verticalAlign": "top", "containerId": None,
              "originalText": s, "lineHeight": 1.25, "autoResize": True, "roundness": None})
    els.append(e)
    return e["id"]


def arrow(x1, y1, x2, y2, color="#1e1e1e", dashed=False, label=None, lsize=14):
    e = _base(x1, y1, abs(x2 - x1) or 1, abs(y2 - y1) or 1, color, "transparent", type="arrow")
    e.update({"id": f"ar{_i[0]}", "points": [[0, 0], [x2 - x1, y2 - y1]],
              "lastCommittedPoint": None, "startBinding": None, "endBinding": None,
              "startArrowhead": None, "endArrowhead": "arrow", "roundness": {"type": 2},
              "strokeWidth": 3})
    if dashed:
        e["strokeStyle"] = "dashed"
    els.append(e)
    if label:
        text((x1 + x2) / 2 - len(label) * 3.4, (y1 + y2) / 2 - 26, label, size=lsize, color=color)
    return e["id"]


WEST_S, WEST_BG = "#1971c2", "#e7f5ff"
EAST_S, EAST_BG = "#2f9e44", "#ebfbee"
ENG_S, ENG_BG = "#e8590c", "#fff0e6"
CTL_S, CTL_BG = "#f08c00", "#fff9db"

text(360, 40, "Model DR — Cross-Workspace Pull (at a glance)", size=30, bold=True)
text(360, 86, "The DR job runs in the secondary and PULLS from the primary. No cross-region S3 copy.", size=16, color="#495057")

# 1. PRIMARY registry (source)
rect(120, 200, 360, 200, WEST_S, WEST_BG)
text(150, 218, "PRIMARY · us-west-2", size=20, color=WEST_S, bold=True)
text(150, 256, "MLflow Model Registry\n\ndr_poc.ml.iris_dr_model\nv1..vN  (+ runs, grants,\nserving endpoint)", size=15, color="#1e1e1e")

# 2. East bucket (landing)
rect(770, 220, 300, 160, EAST_S, "#ffffff")
text(795, 238, "DBFS bucket (east)", size=17, color=EAST_S, bold=True)
text(795, 272, "exported model lands\nhere directly\n(local to secondary)", size=14, color="#495057")

# 3. SECONDARY registry (dest)
rect(1360, 200, 360, 200, EAST_S, EAST_BG)
text(1390, 218, "SECONDARY · us-east-1", size=20, color=EAST_S, bold=True)
text(1390, 256, "MLflow Model Registry\n\nsame models imported\nv1..vN  (+ runs, grants,\nendpoint in standby)", size=15, color="#1e1e1e")

# DR engine (under the flow)
rect(120, 470, 1600, 120, ENG_S, ENG_BG)
text(150, 486, "DR job (runs in SECONDARY, as ad-dr-spn)", size=18, color=ENG_S, bold=True)
text(150, 520, "Authenticates to the primary via a secret scope. EXPORT phase wears the WEST identity to read the source;\n"
               "IMPORT phase wears the local EAST identity to write. Engine = mlflow-export-import. Baseline once, then incremental CDC.",
     size=14, color="#495057")

# Control plane
rect(120, 640, 1600, 90, CTL_S, CTL_BG)
text(150, 654, "Control plane (dr_control)", size=17, color=CTL_S, bold=True)
text(150, 686, "dr_replication_audit = every action + CDC watermark   ·   dr_state = who is primary now (failover/failback flips it)",
     size=14, color="#495057")

# Flow arrows
arrow(480, 300, 770, 300, color=ENG_S, label="1 · EXPORT (remote identity)")
arrow(1070, 300, 1360, 300, color=EAST_S, label="2 · IMPORT (local identity)")

# Failover / failback band
rect(120, 770, 1600, 150, "#e03131", "#fff5f5")
text(150, 784, "FAILOVER  &  FAILBACK", size=20, color="#e03131", bold=True)
text(150, 822, "FAILOVER (in EAST): promote east (already warm), scale up endpoint, dr_state → east. Repoint consumers.", size=14, color="#495057")
text(150, 852, "FAILBACK (in WEST): reverse pull east → west to recover outage-time versions, dr_state → west.", size=14, color="#495057")
text(150, 886, "Same code both ways — direction comes from dr_state, not hardcoded.", size=13, color="#e03131")

arrow(490, 430, 1355, 430, color=EAST_S, label="steady state  west → east")
arrow(1355, 455, 490, 455, color="#e03131", dashed=True, label="failback  east → west")

doc = {"type": "excalidraw", "version": 2, "source": "databricks-dr",
       "elements": els, "appState": {"gridSize": None, "viewBackgroundColor": "#ffffff"}, "files": {}}
out = os.path.join(os.path.dirname(__file__), "architecture_simple.excalidraw")
with open(out, "w") as f:
    json.dump(doc, f, indent=2)
print("wrote", out, "with", len(els), "elements")
