# Deltasearch Business Model

## The Thesis

DuckDB proved you don't need a data warehouse server. Deltasearch proves you don't need a search cluster.

Fully open-source engine, zero data hosting liability, customer brings their own storage. The business is managed freshness — keeping search indexes current against Delta Lake tables in the customer's cloud.

## How It Works

Deltasearch is a single binary. Download it, point it at a Delta table, search. No cluster, no daemon, no JVM. The index is a disposable cache that rebuilds itself from Delta Lake.

```
Customer's cloud (OneLake / ADLS / S3)
├── Delta table (source of truth — customer owns this)
└── tantivy index (disposable cache — rebuilt from Delta)

Compact worker: polls Delta → creates index segments → merges them
Search client: reads index → returns results
```

The search client is stateless and free. The compact worker is where the operational value lives — it keeps the index fresh, handles compaction, and manages the segment lifecycle.

## Pricing

### Free Tier (Community)

Everything is open-source. No limits, no call home, no license key.

- `dsrch` binary — unlimited search, unlimited indexes
- Compact worker — self-operated, no support
- Freshness — best effort (depends on how they run the worker)
- Support — GitHub issues

This is the adoption funnel. Engineers discover deltasearch, use it for ad-hoc search against their lakehouse, realize they want it in production.

### Team ($200/month)

Managed compact workers running in the customer's cloud. We operate the workers; the customer brings storage and credentials.

- Up to 5 managed indexes
- Freshness guarantee: data searchable within 5 minutes of landing in Delta
- Hosted Grafana dashboard (gap size, segment count, worker health)
- Email support, 24hr response
- OneLake / Fabric setup assistance

### Enterprise (custom pricing)

Production SLAs for organizations replacing Elasticsearch.

- Unlimited managed indexes
- Freshness guarantee: data searchable within 60 seconds
- SLA-backed with contractual uptime commitments
- Dashboard + alerting + incident response
- Dedicated support, 4hr response
- Architecture review and onboarding
- Multi-department deployment (labs, meds, encounters, radiology — each a separate index)

## Unit Economics

### Our Cost Per Customer

The compact worker is a single container:
- Polls Delta every 10 seconds (one API call)
- Reads new Parquet files when they appear
- Writes tantivy segments
- Sits idle most of the time

Azure Container Apps consumption pricing: **~$10-15/month per worker.**

### Revenue Per Customer

| Tier | Monthly | Annual |
|------|---------|--------|
| Team (5 indexes) | $200 | $2,400 |
| Enterprise (est. 10 indexes) | $1,000+ | $12,000+ |

### Gross Margin

Team: $200 revenue - $15 cost = **92% gross margin**
Enterprise: $1,000 revenue - $50 cost = **95% gross margin**

## Why This Works

### vs. Elasticsearch Managed Services

| | Elastic Cloud | AWS OpenSearch | Deltasearch |
|--|--------------|----------------|-------------|
| Smallest deployment | ~$95/month | ~$80/month | Free (self-operated) |
| Production deployment | $500-5,000/month | $500-5,000/month | $200/month |
| Ops burden | Cluster management, JVM tuning, shard allocation | Same | One container, restart=always |
| Failure mode | Search down (red shards) | Search down | Search slow (ephemeral index covers gap) |
| Data residency | Their cloud | AWS regions | Customer's own storage |
| HIPAA/compliance | BAA required with vendor | BAA required | No BAA needed — we never see data |

### The Compliance Advantage

Deltasearch never hosts, stores, or transmits customer data. The compact worker runs in the customer's cloud with credentials they provision. We operate compute, not data.

For healthcare customers this eliminates:
- Business Associate Agreements for the search layer
- Data residency concerns
- PHI exposure risk from the search vendor
- Security review of a third-party data store

The customer's InfoSec team reviews one container with read access to their Delta table. That's it.

### The MotherDuck Parallel

| | MotherDuck (DuckDB) | Deltasearch (tantivy) |
|--|--------------------|-----------------------|
| Open-source engine | DuckDB | deltasearch |
| What you pay for | Cloud compute + storage + users | Managed workers + freshness SLA |
| Customer data | Stored on MotherDuck | Stays on customer's storage |
| Lock-in | Data on their platform | Zero — open formats throughout |
| Gross margin | High (hosting markup) | Higher (no data hosting cost) |

MotherDuck proved the model: take a best-in-class embedded engine, add a cloud management layer, charge for operational convenience. Deltasearch applies the same model to search, with an even cleaner cost structure because we don't host data.

### Zero Lock-In

If a customer churns:
- They keep their Delta table (it was always theirs)
- They keep their index files (standard tantivy format)
- They keep the `dsrch` binary (open-source, MIT/Apache-2.0)
- They can run the compact worker themselves

The lock-in is entirely on operational convenience — they don't want to monitor and manage the worker. Not on data, not on format, not on tooling.

## Go-To-Market

### Adoption Funnel

```
Engineer finds dsrch → searches their lakehouse → tells team
    ↓
Team uses it for ad-hoc queries → wants it in production
    ↓
"Who's going to run the compact worker?" → Team tier
    ↓
Multiple departments adopt → Enterprise tier
```

### Initial Target

Mid-size health systems (200-1000 beds) with:
- Data already in Azure / OneLake / Fabric
- MUMPS or Epic EHR with existing blob export pipelines
- Elasticsearch fatigue (cost, ops, complexity)
- Compliance-sensitive (HIPAA, data residency requirements)

### Wedge Use Case

Clinical lab search. Every health system has millions of lab results. Finding a specific patient's labs across time is a daily workflow for care coordinators, compliance teams, and analysts. Today this is either slow (Spark/notebook queries) or expensive (Elasticsearch cluster).

Deltasearch makes it fast, cheap, and operationally simple.

## Revenue Projections (Illustrative)

| Year | Customers | Avg. Revenue | ARR |
|------|-----------|-------------|-----|
| 1 | 10 | $3,600 | $36K |
| 2 | 50 | $6,000 | $300K |
| 3 | 200 | $8,000 | $1.6M |

Conservative assumptions: mostly Team tier in year 1, growing Enterprise mix over time. Zero marketing spend — open-source adoption drives inbound.

## Open Questions

1. **License choice.** Apache-2.0 (maximally permissive) vs. BSL/SSPL (prevents cloud providers from offering it as a service). The trend in infra is toward protective licenses (Elastic, MongoDB, HashiCorp all switched). But DuckDB stayed MIT and MotherDuck is thriving.

2. **Self-hosted enterprise.** Some large health systems won't let external vendors operate anything in their cloud. Offer a self-hosted enterprise tier with support + SLA but no managed operations? This is pure support revenue.

3. **Multi-tenant.** One compact worker serving multiple indexes for different customers? Or strict isolation (one worker per customer)? Isolation is simpler and safer for healthcare. Multi-tenant is more efficient.

4. **Expansion revenue.** Beyond freshness, what else do customers pay for? Schema consulting, custom tokenizers, integration support, training. These are services revenue, not SaaS — lower margin but good for early cash flow.
