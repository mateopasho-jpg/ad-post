# Meta Ads Test Project (Python)

This repo is a **Meta Marketing API test harness** plus a **minimal HTTP API** wrapper so tools like Make.com / Notion can trigger launches.

It supports:
- a local CLI for testing
- an API mode for deployment
- local idempotency (`.meta_idempotency.db`)

---

## Install

```bash
python -m venv .venv
source .venv/bin/activate  # (Windows: .venv\Scripts\activate)

pip install -r requirements.txt
```

## Configure

Copy the env template:

```bash
cp config.example.env .env
```

Fill in at least:
- `META_ACCESS_TOKEN`
- `META_AD_ACCOUNT_ID` (can be `act_123...` or just `123...`)

Optional (recommended):
- `SERVICE_API_KEY` (if set, /run requires header `X-API-Key: <SERVICE_API_KEY>`)
- `IDEMPOTENCY_DB_PATH` (path for `.meta_idempotency.db`)
- `QUEUE_DB_PATH` (path for `.queue_state.db`)

---

## Quick start

```bash
python meta_ads_tool.py whoami
python meta_ads_tool.py list-adaccounts
python meta_ads_tool.py adaccount
```

---

## Run as an API (for Make.com)

Start the server:

```bash
uvicorn api:app --host 0.0.0.0 --port 8080
```

Health check:

```bash
curl http://localhost:8080/health
```

Run a launch (send a LaunchPlan JSON inside the `plan` field):

```bash
curl -X POST http://localhost:8080/run \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: YOUR_SERVICE_API_KEY' \
  -d '{
    "job_id": "local-test",
    "dry_run": true,
    "plan": '"$(cat examples/traffic_image_ad_v2.json)"'
  }'
```

Notes:
- If you do not set `SERVICE_API_KEY`, you can omit the `X-API-Key` header.
- For automation (Notion/Make), prefer `assets.media_url` (image **or** video).
  - If you set `assets.media_type` to `video`, the backend will upload a video and inject `video_id`.
  - If you omit `assets.media_type`, the backend will auto-detect from the URL/content-type.
  - For video creatives, `creative.object_story_spec` should contain `video_data` (preferred).


## Worker (recommended for Railway)

Schema v2 uses a **time-based fallback** (e.g. allow 3 items after 60s).
If no new webhook arrives after the 3rd item, you still want the batch to flush — that’s what the worker does.

Run the worker locally:

```bash
python worker.py
```

### Railway setup

Create **two services** from the same repo:

1) **API service** (web)
- Start command: `uvicorn api:app --host 0.0.0.0 --port $PORT`

2) **Worker service**
- Start command: `python worker.py`

Both services must share the same `DATABASE_URL` and should use Postgres stores.

To avoid races, disable immediate draining in the API service when a worker is running:

```bash
ENABLE_IMMEDIATE_DRAIN=false
```

Then enable Postgres stores:


```bash
QUEUE_STORE_SOURCE=db
IDEMPOTENCY_STORE_SOURCE=db
```


---

## Schema v2 (current) — naming + batching

> Use `schema_version: 2` in the LaunchPlan.

### Countries field (Notion multi-select)

You can provide countries either as a list or a comma-separated string:

```json
"countries": ["AT", "DE"]
```

This automatically overwrites:
`adset.targeting.geo_locations.countries`.

### Naming convention

**Campaign name**

```
VFB // <Product Label> // #<Product Code># // <Variant Label> // WC // ABO // Lowest Cost
```

Variant labels default to:
- green: `TESTING`, `GRAFIK TESTING`
- lila: `TESTING`, `GRAFIK TESTING`
- rosa: `TESTING`

**Ad Set name**

```
<NNNN> Test // VFB // #<Product Code># // <Product Label> // <Audience> // Batch
```

`<NNNN>` is auto-incremented by scanning existing AdSets in the product's campaign(s).

**Ad name**

```
<NotionNumber>_<Variant>_<Product Label> // Video // Mehr dazu // <Offer Page>
```

### AdSet creation logic (3–4 ads, category separation)

- Items are queued per `(product, category, adset_signature)`.
- An AdSet is created when either:
  - **4** unique video IDs are available (default target), OR
  - **3** unique video IDs are available **and** the oldest item in the group has been queued for at least `BATCH_FALLBACK_AFTER_S` seconds (default **60s**).
- Each AdSet contains **up to 4** unique video IDs.
- Remaining items are automatically split into the next AdSet.
- `category` must be `ai` or `ug`. AI and UG are **never mixed** in the same AdSet.

**Video variants rule**

The backend treats the leading number in `creative.name` as the **video identifier** (e.g. `3452` in `3452_0_...`).
Variants of the same video (e.g. `3452_0` and `3452_1`) are **not allowed in the same AdSet**.
If duplicates are waiting in the queue, the API response will include:

> `versions of the same video cannot go in the same AdSet`

### Campaign selection for green/lila (50/50 split)

- green and lila each use **two active campaigns**.
- When creating a new AdSet, the backend picks the campaign with **fewer existing AdSets** (keeps them balanced).
- rosa uses only one campaign.

### Campaign ID DB (Railway Postgres)

If you want to pin campaign IDs (recommended in production), set:

```bash
CAMPAIGN_ROUTE_SOURCE=db
DATABASE_URL=postgres://...
```

Create/overwrite mappings by inserting rows into:

`product_campaign_routes_v2(product, slot, campaign_id)`

- `slot=1` and `slot=2` for green/lila
- `slot=1` for rosa

The code always checks this DB **first**.

### Postgres queue + idempotency (recommended long-term on Railway)

By default the project stores:
- idempotency in a local SQLite file (`IDEMPOTENCY_DB_PATH`)
- schema v2 batching queue in a local SQLite file (`QUEUE_DB_PATH`)

For long-term Railway deployments (no volumes, safe restarts, optional multi-replica), switch both to Postgres:

```bash
DATABASE_URL=postgres://...
QUEUE_STORE_SOURCE=db
IDEMPOTENCY_STORE_SOURCE=db
```

Run this SQL once in your Railway Postgres:

```sql
CREATE TABLE IF NOT EXISTS queue_v2 (
  id BIGSERIAL PRIMARY KEY,
  product TEXT NOT NULL,
  category TEXT NOT NULL,
  signature TEXT NOT NULL,
  video_id TEXT NOT NULL,
  payload_json JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  state TEXT NOT NULL DEFAULT 'queued',
  reserved_at TIMESTAMPTZ,
  reserved_by TEXT
);

CREATE INDEX IF NOT EXISTS idx_queue_v2_group
  ON queue_v2(product, category, signature, id);

CREATE TABLE IF NOT EXISTS launches_v2 (
  launch_key TEXT PRIMARY KEY,
  payload_sha256 TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  campaign_id TEXT,
  adset_id TEXT,
  creative_id TEXT,
  ad_id TEXT
);

CREATE TABLE IF NOT EXISTS campaign_cache (
  product TEXT PRIMARY KEY,
  campaign_name TEXT NOT NULL,
  campaign_id TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS adset_cache (
  campaign_id TEXT NOT NULL,
  batch_id TEXT NOT NULL,
  bucket INTEGER NOT NULL,
  adset_name TEXT NOT NULL,
  adset_id TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (campaign_id, batch_id, bucket)
);
```

Notes:
- The queue uses a lightweight reservation mechanism to avoid double-processing.
- If a request crashes mid-run, reserved rows automatically become eligible again after ~30 minutes.

### URL parameters (hard-coded)

Every creative gets these URL parameters appended automatically:

```
trc_mcmp_id={{campaign.id}}&trc_mag_id={{adset.id}}&trc_mad_id={{ad.id}}
```

---

## Local dry run and full run

Dry run (no Meta writes; safe):

```bash
python meta_ads_tool.py launch --plan examples/traffic_image_ad_v2.json --dry-run
```

Full run:

```bash
python meta_ads_tool.py launch --plan examples/traffic_image_ad_v2.json
```

> In schema v2 batching mode, each call **enqueues one item** and will only create an AdSet once 3+ unique videos are queued for that group.

---

## Useful helper commands

Find your Page ID / IG actor ID (optional but usually needed for creatives):

```bash
python meta_ads_tool.py promote-pages
python meta_ads_tool.py instagram-accounts
```

Upload an image:

```bash
python meta_ads_tool.py upload-image --image-path ./my_image.jpg
```

Pause/Activate an object:

```bash
python meta_ads_tool.py set-status --id <ID> --status PAUSED
python meta_ads_tool.py set-status --id <ID> --status ACTIVE
```

Update AdSet budget:

```bash
python meta_ads_tool.py update-adset-budget --adset-id <ADSET_ID> --daily-budget 1500
```

---

## Docker (deploy anywhere)

Build:

```bash
docker build -t meta-ads-tool:latest .
```

Run:

```bash
docker run --rm -p 8080:8080 --env-file .env meta-ads-tool:latest
```

### Video upload controls

Environment variables:
- `VIDEO_WAIT_FOR_READY` (default `true`) — wait until Meta finishes encoding before creating the ad
- `VIDEO_WAIT_TIMEOUT_S` (default `600`)
- `VIDEO_WAIT_POLL_S` (default `5`)
