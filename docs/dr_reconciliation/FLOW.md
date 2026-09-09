# DR Reconciliation — Complete Flow

Two diagrams: (1) the end-to-end architecture/data flow, (2) what one recon run does.

## 1. Architecture & data flow

```mermaid
flowchart TB
    subgraph ACCT["Account control plane (gated: Managed DR)"]
      DRAPI["DR REST API<br/>state · effective_primary_region<br/>(real-time, no lag)"]
      SYS["system.replication.states<br/>RPO lag · errors[]<br/>(system table, ≤3h lag)"]
    end

    subgraph EAST["PRIMARY · us-east-1 (ACTIVE, writable)"]
      SEED["Seeded workspace assets<br/>notebooks · file · job(paused) · warehouse · draft dashboard"]
      EWH["Serverless warehouse<br/>cc56ad79...  (ALL recon SQL runs here)"]
      ENGINE["RECON ENGINE  (recon_engine.py)<br/>laptop now · serverless job when egress allowlisted"]
      CTL["dr_recon.control.*<br/>runs · coverage · inventory · findings · audit · events"]
      DASH["AI/BI Dashboard"]
      APP["Databricks App<br/>console + actions + Ask Genie"]
    end

    subgraph WEST["SECONDARY · us-west-2 (READ-ONLY, NO compute)"]
      WASSETS["Replicated workspace assets<br/>(read via control-plane REST only)"]
    end

    SEED -->|"Managed DR replicates<br/>(continuous, east→west)"| WASSETS

    ENGINE -->|"read PRIMARY assets (REST)"| SEED
    ENGINE -->|"read SECONDARY assets (REST, no compute)"| WASSETS
    DRAPI -->|"fresh state"| ENGINE
    SYS -->|"RPO lag + errors"| ENGINE
    ENGINE -->|"diff · classify · MERGE"| CTL
    CTL --> DASH
    CTL --> APP
    EWH -. "executes SQL" .- ENGINE
    EWH -. "queries" .- DASH
    EWH -. "queries" .- APP

    classDef pri fill:#e7f5ff,stroke:#1971c2;
    classDef sec fill:#f3f0ff,stroke:#6741d9;
    classDef acct fill:#ebfbee,stroke:#2f9e44;
    class EAST,SEED,EWH,ENGINE,CTL,DASH,APP pri;
    class WEST,WASSETS sec;
    class ACCT,DRAPI,SYS acct;
```

**Key points the diagram encodes**
- Managed DR does the **replication** (east→west, continuous). Our recon does the **verification**.
- The recon engine reads **both** workspaces, but the secondary only via **control-plane REST** →
  **no compute runs on west**. All recon **SQL** runs on the **east** warehouse.
- **State** comes from the **DR API** (fresh); **RPO lag + errors** from the **system table** (≤3h lag);
  **per-object parity** from **live reads** (real-time).
- Everything lands in `dr_recon.control.*`, consumed by the **dashboard** and the **app (+Genie)**.

## 2. What one recon run does

```mermaid
flowchart TD
    A["Start run<br/>(15-min cadence / on-demand / after failover)"] --> B["Get failover-group state<br/>DR API + system.replication.states"]
    B --> C{"Secondary reachable?<br/>(preflight)"}
    C -->|No| X["Record UNKNOWN run<br/>+ SECONDARY_UNREACHABLE finding<br/>(never false MISSING)"] --> Z["Stop"]
    C -->|Yes| D["Read 7 workspace-asset types<br/>from PRIMARY and SECONDARY (live)"]
    D --> E["Diff per object by signature<br/>content/config/schedule/ACL + secondary dormant state"]
    E --> F["Classify each object<br/>IN_SYNC · LAGGING · DRIFTED · MISSING · FAILED · UNSUPPORTED"]
    F --> G{"Managed DR state?"}
    G -->|INITIAL_REPLICATION| H["BASELINE mode<br/>record all, SUPPRESS alarms<br/>readiness = BOOTSTRAP"]
    G -->|ACTIVE| I["ASSURANCE mode<br/>findings + alerts<br/>readiness GREEN / AT_RISK / CRITICAL"]
    H --> J["Also fold in Managed DR errors[]<br/>+ incremental audit (NEW/CHANGED/REMOVED)"]
    I --> J
    J --> K["Write dr_recon.control.*"] --> L["Dashboard + App refresh"]

    classDef ok fill:#e6fcf5,stroke:#0ca678;
    classDef warn fill:#fff4e6,stroke:#e8590c;
    class H,I,J,K,L ok; class X warn;
```

## Where we are on this flow right now
- **Seed → Managed DR:** done; `fg-dr-recon` = `INITIAL_REPLICATION` (2 of 6 replicated so far).
- **Recon:** runs correctly from an allowlisted host → **BASELINE** mode (job `IN_SYNC`, rest `MISSING` — expected mid-bootstrap).
- **Outputs:** dashboard + app + Genie all live over `dr_recon.control.*`.
- **Pending:** Managed DR → `ACTIVE` (then recon auto-flips to **ASSURANCE**); allowlist the serverless
  egress on west to run the scheduled job server-side. See [START_HERE.md](START_HERE.md).
