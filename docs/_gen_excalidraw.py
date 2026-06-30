"""Generate docs/architecture.excalidraw for the model-DR cross-workspace pull design.

Run: python docs/_gen_excalidraw.py   (writes docs/architecture.excalidraw)
This is a one-shot authoring helper; the .excalidraw file is the artifact.
"""
import json
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


def rect(x, y, w, h, stroke="#1e1e1e", bg="transparent", dashed=False):
    e = _base(x, y, w, h, stroke, bg, type="rectangle")
    e["id"] = f"r{_i[0]}"
    if dashed:
        e["strokeStyle"] = "dashed"
    els.append(e)
    return e["id"]


def text(x, y, s, size=16, color="#1e1e1e", w=None, align="left", bold=False):
    lines = s.split("\n")
    width = w or (max(len(ln) for ln in lines) * size * 0.6)
    height = len(lines) * size * 1.25
    e = _base(x, y, width, height, color, "transparent", type="text")
    e.update({
        "id": f"t{_i[0]}", "text": s, "fontSize": size,
        "fontFamily": 2 if not bold else 5, "textAlign": align,
        "verticalAlign": "top", "containerId": None, "originalText": s,
        "lineHeight": 1.25, "autoResize": True, "roundness": None,
    })
    els.append(e)
    return e["id"]


def arrow(x1, y1, x2, y2, color="#1e1e1e", dashed=False, label=None, lsize=13):
    w = x2 - x1
    h = y2 - y1
    e = _base(x1, y1, abs(w) or 1, abs(h) or 1, color, "transparent", type="arrow")
    e.update({
        "id": f"ar{_i[0]}", "points": [[0, 0], [w, h]],
        "lastCommittedPoint": None, "startBinding": None, "endBinding": None,
        "startArrowhead": None, "endArrowhead": "arrow", "roundness": {"type": 2},
        "strokeWidth": 2.5,
    })
    if dashed:
        e["strokeStyle"] = "dashed"
    els.append(e)
    if label:
        text((x1 + x2) / 2 - len(label) * 3.2, (y1 + y2) / 2 - 22, label, size=lsize, color=color)
    return e["id"]


# Palettes
WEST_S, WEST_BG = "#1971c2", "#e7f5ff"
EAST_S, EAST_BG = "#2f9e44", "#ebfbee"
CTL_S, CTL_BG = "#f08c00", "#fff9db"
ENG_S, ENG_BG = "#e8590c", "#fff0e6"
SEC_S, SEC_BG = "#9c36b5", "#f8f0fc"

# Title
text(700, 30, "Databricks Model DR — Cross-Workspace Pull (no CRR)", size=30, color="#1e1e1e", bold=True)
text(700, 78, "Replicates UC models + versions, experiments/runs, grants & serving endpoints. Audit + dr_state control plane.", size=16, color="#495057")

# ---------- EAST (PRIMARY) ----------
rect(60, 140, 900, 940, WEST_S, WEST_BG)
text(90, 158, "PRIMARY · us-east-1", size=22, color=WEST_S, bold=True)
text(90, 192, "fe-sandbox-krish-us-eat-1-sandbox   ·   metastore: ad-dr-metastore-us-east-1", size=13, color="#495057")

# East UC / registry
rect(95, 235, 540, 300, WEST_S, "#ffffff")
text(115, 248, "Unity Catalog  ·  catalog: dr_poc", size=15, color=WEST_S, bold=True)
rect(120, 285, 490, 95, WEST_S, WEST_BG)
text(135, 296, "MLflow Model Registry  (schema: ml)", size=14, bold=True)
text(135, 322, "dr_poc.ml.iris_dr_model  →  v1, v2, … vN\naliases · model-version tags · permissions", size=12, color="#495057")
rect(120, 392, 490, 120, WEST_S, WEST_BG)
text(135, 402, "Experiments · Runs · Artifacts", size=14, bold=True)
text(135, 428, "backing run (params/metrics)\nmodel artifacts in UC managed storage (S3)\nsignature · input_example", size=12, color="#495057")

# East control plane
rect(655, 235, 280, 300, CTL_S, "#ffffff")
text(675, 248, "Control plane  ·  dr_control", size=14, color=CTL_S, bold=True)
rect(675, 288, 240, 95, CTL_S, CTL_BG)
text(688, 298, "dr_replication_audit", size=13, bold=True)
text(688, 322, "every EXPORT/IMPORT/VERIFY\nGRANTS/ENDPOINT/HEALTH\n+ CDC watermark", size=11, color="#495057")
rect(675, 398, 240, 110, CTL_S, CTL_BG)
text(688, 408, "dr_state (single row)", size=13, bold=True)
text(688, 432, "active_primary = east\nfailover/failback flips it\n(read by every job run)", size=11, color="#495057")

# East serving endpoint
rect(95, 560, 400, 110, WEST_S, "#ffffff")
text(115, 572, "Model Serving endpoint (ACTIVE)", size=14, bold=True)
text(115, 598, "iris-dr-endpoint  →  dr_poc.ml.iris_dr_model:vN\nserves consumer REST traffic", size=12, color="#495057")

# East DBFS
rect(515, 560, 420, 110, WEST_S, "#ffffff")
text(535, 572, "Source artifact storage (primary)", size=14, bold=True)
text(535, 598, "UC managed storage (S3)\n(source artifacts read in EXPORT phase)", size=12, color="#495057")

# East failback secret scope (used only on failback)
rect(95, 700, 840, 90, SEC_S, "#ffffff")
text(115, 712, "Secret scope: dr_remote_west   (used only during FAILBACK)", size=13, color=SEC_S, bold=True)
text(115, 738, "host + ad-dr-spn PAT for WEST  →  lets EAST pull west→east to recover outage-time versions", size=12, color="#495057")

text(95, 820, "Identity: ad-dr-spn (account-admin SPN, present in both workspaces)", size=12, color="#495057")
text(95, 980, "On FAILBACK this region is the DESTINATION:\nthe DR job runs HERE, pulls west→east, resets dr_state=east.", size=12, color=WEST_S)

# ---------- WEST (SECONDARY) ----------
EX = 1480
rect(EX, 140, 920, 940, EAST_S, EAST_BG)
text(EX + 30, 158, "SECONDARY · us-west-2   (steady-state DR runs HERE)", size=22, color=EAST_S, bold=True)
text(EX + 30, 192, "fe-sandbox-ankita-dr-wp-us-west-2   ·   metastore: ad-dr-metastore-us-west-2", size=13, color="#495057")

# DR engine / jobs box
rect(EX + 30, 230, 860, 215, ENG_S, "#ffffff")
text(EX + 50, 242, "DR jobs (Databricks Asset Bundle, run_as ad-dr-spn)", size=15, color=ENG_S, bold=True)
text(EX + 50, 270,
     "dr_models_bootstrap   ·   dr_models_replicate (baseline)\n"
     "dr_models_cdc  →  health   (scheduled 15 min, watermark-gated)\n"
     "dr_models_health (hourly scan)   ·   dr_models_failover\n"
     "drill_failover / drill_failback   ·   alerts on FAILED rows",
     size=12, color="#495057")
rect(EX + 50, 372, 820, 58, ENG_S, ENG_BG)
text(EX + 62, 382,
     "engine = native (MLflow client + databricks-sdk). EXPORT becomes remote (EAST) identity;\nIMPORT restores local (WEST) identity. Per-model export→import + verify.",
     size=11, color="#495057")

# West secret scope
rect(EX + 30, 460, 860, 78, SEC_S, "#ffffff")
text(EX + 50, 470, "Secret scope: dr_remote_east   (steady-state)", size=13, color=SEC_S, bold=True)
text(EX + 50, 496, "host + ad-dr-spn PAT for EAST  →  the job authenticates to the primary to read its registry", size=12, color="#495057")

# West DBFS
rect(EX + 30, 556, 410, 110, EAST_S, "#ffffff")
text(EX + 50, 568, "Staging Volume (local / secondary)", size=14, bold=True)
text(EX + 50, 594, "dr_poc.dr_control.dr_staging  (S3-backed UC Volume)\nexports land HERE (no cross-region copy)", size=12, color="#495057")

# West registry
rect(EX + 460, 556, 430, 220, EAST_S, "#ffffff")
text(EX + 480, 568, "MLflow Model Registry (WEST)", size=14, bold=True)
text(EX + 480, 594, "dr_poc.ml.iris_dr_model  →  v1..vN (imported)\nexperiments under /Shared/dr/experiments\ngrants mirrored (USE CATALOG/SCHEMA/EXECUTE)", size=12, color="#495057")
rect(EX + 480, 678, 390, 78, EAST_S, EAST_BG)
text(EX + 492, 688, "Serving endpoint (STANDBY → ACTIVE)", size=13, bold=True)
text(EX + 492, 712, "iris-dr-endpoint mirrored scale-to-zero;\nfailover scales it up to serve", size=11, color="#495057")

# West control plane (mirror)
rect(EX + 30, 690, 410, 86, CTL_S, "#ffffff")
text(EX + 50, 700, "dr_control (WEST): audit + dr_state", size=13, color=CTL_S, bold=True)
text(EX + 50, 724, "independent copy; queryable after failover.\nwatermark drives incremental CDC.", size=11, color="#495057")

text(EX + 30, 980, "Steady state: this region is the DESTINATION.\nThe job pulls east→west; dr_state stays 'east'.", size=12, color=EAST_S)

# ---------- FLOW ARROWS (center) ----------
# 1. EXPORT: WEST job reads EAST registry (remote identity)
arrow(EX + 30, 320, 615, 330, color=ENG_S, label="1 · EXPORT  (ambient = EAST via dr_remote_east)")
# 2. artifacts to WEST bucket
arrow(615, 470, EX + 235, 556, color="#1e1e1e", label="2 · artifacts → local staging Volume")
# 3. IMPORT into WEST registry
arrow(EX + 235, 620, EX + 460, 620, color=EAST_S, label="3 · IMPORT (local)")
# 4. grants + endpoints mirror
arrow(495, 615, EX + 460, 705, color="#1e1e1e", dashed=True, label="4 · grants + endpoint mirror (standby)")
# 5. audit/state writes
arrow(EX + 230, 445, EX + 230, 690, color=CTL_S, dashed=True, label="5 · audit + dr_state")

# Failover / failback band
rect(60, 1110, 2340, 150, "#e03131", "#fff5f5")
text(90, 1122, "FAILOVER  &  FAILBACK", size=20, color="#e03131", bold=True)
text(90, 1158,
     "FAILOVER (run in WEST):  no pull (primary may be down) — west already warm; scale up endpoint; write FAILOVER row; dr_state → west.  Repoint consumers.",
     size=13, color="#495057")
text(90, 1188,
     "FAILBACK (run in EAST):  reverse CDC west→east via dr_remote_west pulls outage-time versions; write FAILBACK row; dr_state → east.  Steady state restored.",
     size=13, color="#495057")
text(90, 1222,
     "Direction is parameterized in Config.direction(): env override > dr_state table > config role. Same code runs both ways — no hardcoded east→west.",
     size=12, color="#e03131")

# normal direction arrow (east primary -> west)
arrow(970, 1075, EX + 20, 1075, color=EAST_S, label="steady-state CDC  east → west", lsize=14)
# failback arrow
arrow(EX + 20, 1095, 970, 1095, color="#e03131", dashed=True, label="failback  west → east", lsize=14)

doc = {"type": "excalidraw", "version": 2, "source": "databricks-dr",
       "elements": els,
       "appState": {"gridSize": None, "viewBackgroundColor": "#ffffff"},
       "files": {}}

import os
out = os.path.join(os.path.dirname(__file__), "architecture.excalidraw")
with open(out, "w") as f:
    json.dump(doc, f, indent=2)
print("wrote", out, "with", len(els), "elements")
