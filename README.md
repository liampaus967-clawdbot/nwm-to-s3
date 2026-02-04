# NWM to S3 Pipeline

Fetches National Water Model (NWM) velocity and streamflow data from NOAA and uploads it to S3 as JSON for the Streamflow Viz app.

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  NOAA NWM S3    │────▶│  This Script    │────▶│  Your S3 Bucket │
│  (NetCDF files) │     │  (Python)       │     │  (JSON output)  │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                        │
                                                        ▼
                                                ┌─────────────────┐
                                                │  Mapbox App     │
                                                │  (feature-state)│
                                                └─────────────────┘
```

## Setup

### 1. Create S3 Bucket (Terraform)

```bash
cd terraform
terraform init
terraform plan
terraform apply
```

This creates:
- `nwm-streamflow-data` bucket in us-east-1
- Public read access for `/live/*` path
- CORS configuration for browser access

### 2. Configure Environment

```bash
cp .env.example .env
# Edit .env with your AWS credentials
```

Or use AWS CLI profile:
```bash
aws configure
```

### 3. Install Dependencies

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 4. Run

```bash
# Dry run (saves locally, doesn't upload)
python fetch_nwm.py --dry-run

# Production run
python fetch_nwm.py
```

## Output Format

The JSON uploaded to S3 looks like:

```json
{
  "generated_at": "2026-02-04T14:00:00Z",
  "reference_time": "2026-02-04T13:00:00Z",
  "site_count": 2700000,
  "sites": {
    "1234567": {
      "velocity_ms": 0.45,
      "streamflow_cms": 12.5,
      "velocity_fps": 1.48,
      "streamflow_cfs": 441.3,
      "velocity_category": "moderate",
      "flow_category": "low"
    },
    ...
  }
}
```

## Categories

### Velocity Categories
| Category | Range (m/s) | Description |
|----------|-------------|-------------|
| very_slow | < 0.1 | Nearly stagnant |
| slow | 0.1 - 0.3 | Easy paddling |
| moderate | 0.3 - 0.6 | Normal flow |
| fast | 0.6 - 1.0 | Moving water |
| very_fast | 1.0 - 2.0 | Swift current |
| extreme | > 2.0 | Dangerous |

### Flow Categories
| Category | Range (m³/s) | Description |
|----------|--------------|-------------|
| very_low | < 1 | Trickle |
| low | 1 - 10 | Small stream |
| moderate | 10 - 50 | Medium river |
| high | 50 - 200 | Large river |
| very_high | 200 - 1000 | Major river |
| extreme | > 1000 | Flooding/dam release |

## Cron Setup

For hourly updates:

```bash
# Add to crontab
0 * * * * cd /home/ubuntu/nwm-to-s3 && /home/ubuntu/nwm-to-s3/venv/bin/python fetch_nwm.py >> /var/log/nwm-to-s3.log 2>&1
```

## Frontend Usage

In your Mapbox app:

```typescript
const S3_URL = "https://nwm-streamflow-data.s3.us-east-1.amazonaws.com/live/current_velocity.json";

// Fetch data
const response = await fetch(S3_URL);
const data = await response.json();

// Apply to map using feature-state
Object.entries(data.sites).forEach(([comid, siteData]) => {
  map.setFeatureState(
    { source: "rivers", sourceLayer: "river-layer", id: comid },
    { 
      velocity: siteData.velocity_ms,
      flow_category: siteData.flow_category 
    }
  );
});
```

Then style with:

```javascript
"line-color": [
  "match",
  ["feature-state", "flow_category"],
  "very_low", "#22c55e",
  "low", "#84cc16",
  "moderate", "#eab308",
  "high", "#f97316",
  "very_high", "#ef4444",
  "extreme", "#dc2626",
  "#3b82f6"  // default
]
```

## TODO

- [ ] Create national river tileset with COMID as promoteId
- [ ] Set up cron job for hourly updates
- [ ] Add CloudWatch alerting for failures
