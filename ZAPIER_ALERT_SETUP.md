# Zapier Alert Setup

This guide configures crawler alerts to send readable HTML emails via Zapier.

## 1. Create Zap Trigger

1. Create a new Zap.
2. Trigger app: `Webhooks by Zapier`.
3. Trigger event: `Catch Hook` (not `Catch Raw Hook`).
4. Leave `Pick off a Child Key` empty.
5. Copy the generated webhook URL.

## 2. Set Webhook URL in Environment

Set `CRAWLER_ALERT_WEBHOOK_URL` to that Zapier hook URL.

- Local `.env`:
  - `CRAWLER_ALERT_WEBHOOK_URL=https://hooks.zapier.com/hooks/catch/.../.../`
- Railway `crawler` service variable:
  - `CRAWLER_ALERT_WEBHOOK_URL=https://hooks.zapier.com/hooks/catch/.../.../`

## 3. Send Test Payload from Repo

From repo root:

```bash
python3 scripts/test_crawler_alert.py --details-json '{"parser":"mfa","parsed_count":0}'
```

Expected local output includes:

- `[ALERT] ...`
- `webhook delivered status=200`

## 4. Load Sample in Zapier

1. Return to Zapier trigger step.
2. Click `Test trigger` / `Find new records`.
3. Confirm fields are present: `title`, `message`, `details`, `timestamp_utc`.

## 5. Add Email Action (HTML)

1. Add action app: `Gmail` (or `Email by Zapier`).
2. Action event: `Send Email`.
3. Set body type to HTML (or use HTML body field).
4. Map fields:
   - Subject -> `title`
   - Body -> `message`, `timestamp_utc`, `details` (or nested detail fields when available)

Use this HTML body template:

```html
<div style="font-family:Arial,Helvetica,sans-serif;max-width:680px;margin:0 auto;color:#1f2937;line-height:1.5;">
  <h2 style="margin:0 0 12px 0;color:#111827;">Crawler Alert</h2>

  <div style="padding:12px 14px;background:#f9fafb;border:1px solid #e5e7eb;border-radius:8px;margin-bottom:14px;">
    <div style="font-size:14px;"><strong>Title:</strong> {{title}}</div>
    <div style="font-size:14px;"><strong>Message:</strong> {{message}}</div>
    <div style="font-size:14px;"><strong>Time (UTC):</strong> {{timestamp_utc}}</div>
  </div>

  <h3 style="margin:0 0 8px 0;font-size:15px;color:#111827;">Details</h3>
  <pre style="white-space:pre-wrap;background:#0b1020;color:#e5e7eb;padding:12px;border-radius:8px;font-size:12px;">{{details}}</pre>
</div>
```

## 6. Publish and Verify

1. Publish/turn on the Zap.
2. Run another test:

```bash
python3 scripts/test_crawler_alert.py --title "Crawler alert test" --message "Second verification run"
```

3. Confirm email arrives.

## Production Behavior

Crawler scripts abort DB writes when `--commit` is set and parsed rows are zero.

- If webhook is configured, alert payload is POSTed to Zapier.
- If webhook is missing, alert still appears in logs and script exits non-zero.

## Troubleshooting

If you only see `Object.to_json(Raw Output)` in field mapping:

1. Re-check trigger is `Catch Hook`, not `Catch Raw Hook`.
2. Re-send a test payload from the script.
3. Re-run `Test trigger`.
4. If still needed, add a `Code by Zapier` step to parse JSON from raw output, then map parsed fields.

### Code by Zapier (JavaScript) parser

Use this when Zapier only exposes a raw JSON blob.

1. Add step: `Code by Zapier` -> `Run Javascript`.
2. Add input field:
   - Key: `raw`
   - Value: map from trigger raw output (the `Object.to_json(Raw Output)` token).
3. Paste code:

```javascript
const obj = typeof inputData.raw === "string"
  ? JSON.parse(inputData.raw)
  : inputData.raw;

return {
  title: obj.title || "",
  message: obj.message || "",
  timestamp_utc: obj.timestamp_utc || "",
  details: JSON.stringify(obj.details || {}),
};
```

4. In the email step, map from this Code step outputs:
   - `title`
   - `message`
   - `timestamp_utc`
   - `details`
