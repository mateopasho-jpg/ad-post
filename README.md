# Meta Ads Test Project (Python)

This project contains a **Meta Marketing API test harness** plus a **minimal HTTP API** wrapper so tools like Make.com can trigger launches.

It supports a local CLI for quick testing, and an API mode for deployment.
- Verify your token + account connections are correct
- Verify you can create/update campaigns/adsets/ads without duplicates
- Only then wire it into Make → Node/Python backend → Notion status sync

## What you can do with this project

- Validate token (GET /me)
- List ad accounts you can access
- Inspect an ad account
- Upload an image and get `image_hash`
- Create:
  - Campaign → AdSet → AdCreative → Ad
- Read objects (campaign/adset/ad/creative)
- Pause/Activate objects
- Update AdSet budget
- Local idempotency via SQLite file (`.meta_idempotency.db`)

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
- optional: `META_API_VERSION` (defaults to v21.0)

Optional (recommended for production):
- `SERVICE_API_KEY` (if set, /run requires header `X-API-Key: <SERVICE_API_KEY>`)
- `IDEMPOTENCY_DB_PATH` (where the SQLite idempotency DB is stored)

## Quick start

```bash
python meta_ads_tool.py whoami
python meta_ads_tool.py list-adaccounts
python meta_ads_tool.py adaccount
```

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
    "plan": '"$(cat examples/traffic_image_ad.json)"'
  }'
```

Notes:
- If you do not set `SERVICE_API_KEY`, you can omit the `X-API-Key` header.
- For automation (Notion/Make), prefer `assets.image_url` over `assets.image_path`.

Routing:
- `plan.product` is required (`green`, `lila`, `rosa`). The backend will **route the ad into the corresponding product campaign**.
- You can configure the canonical campaign names via `PRODUCT_CAMPAIGN_NAMES` (JSON string), e.g.
  `{"green":"GREEN | Main","lila":"LILA | Main","rosa":"ROSA | Main"}`.
- AdSets are auto-chosen from `plan.creative.name` by grouping variants in buckets of 5 (see below).

### Find your Page ID / IG actor ID (optional but usually needed for creatives)

```bash
python meta_ads_tool.py promote-pages
python meta_ads_tool.py instagram-accounts
```

### Upload an image

```bash
python meta_ads_tool.py upload-image --image-path ./my_image.jpg
```

### Launch a full test ad (PAUSED)

1. Open `examples/traffic_image_ad.json`
2. Replace:
   - `REPLACE_WITH_PAGE_ID`
   - `REPLACE_WITH_IG_ACTOR_ID` (optional if you only run FB placements)
   - `REPLACE_WITH_LOCAL_IMAGE_PATH.jpg`
3. Run:

```bash
python meta_ads_tool.py launch --plan examples/traffic_image_ad.json
```

Re-running the same plan will not create duplicates (local idempotency).

### Product routing + adset bucketing

Your LaunchPlan must include a `product` field (`green`, `lila`, or `rosa`).
The backend routes each launch into a *canonical product campaign* (get-or-create by name), and then
places ads into ad sets grouped by 5 creatives based on the pattern in `creative.name` (e.g. `3807_0_...`).

You can override the campaign names via:

```bash
PRODUCT_CAMPAIGN_NAMES='{"green":"GREEN | Main Campaign","lila":"LILA | Main Campaign","rosa":"ROSA | Main Campaign"}'
```

### Using an image URL instead of a local path

In automation flows, you typically won't have access to a local file path. Use `assets.image_url`:

```json
"assets": {
  "image_url": "https://.../your-image.jpg"
}
```

### Pause/Activate an ad

```bash
python meta_ads_tool.py set-status --id <AD_ID> --status PAUSED
python meta_ads_tool.py set-status --id <AD_ID> --status ACTIVE
```

### Update AdSet budget

```bash
python meta_ads_tool.py update-adset-budget --adset-id <ADSET_ID> --daily-budget 1500
```

> Budget values are in the smallest currency unit (for USD: cents).

## Notes

- This is for testing; keep everything `PAUSED` until you’re ready to spend.
- The local idempotency db is *not* a replacement for the Notion idempotency key.
  It just protects you while running tests locally.

## Docker (deploy anywhere)

Build:

```bash
docker build -t meta-ads-tool:latest .
```

Run:

```bash
docker run --rm -p 8080:8080 --env-file .env meta-ads-tool:latest
```

