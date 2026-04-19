# Vercel Frontend Setup

Migrate the Next.js frontend from Railway to Vercel. The frontend is fully client-side rendered (CSR) — Vercel serves it as static assets from a global CDN at no cost.

## Prerequisites

- Repo pushed to GitHub (`thomasruntaochen-ops/art_activity_collections`)
- Railway API public URL (find it in Railway → art_activity_service → Settings → Domains)

## Steps

### 1. Import project into Vercel

1. Go to [vercel.com](https://vercel.com) and sign in with GitHub
2. Click **"Add New Project"**
3. Select the `art_activity_collections` repo
4. Click **Edit** next to **Root Directory** and set it to `frontend`
   - Note: Vercel cannot access files outside this directory (no `..` path traversal)
5. Framework will auto-detect as **Next.js**

### 2. Set environment variable

Before deploying, add in the Vercel project settings under **Environment Variables**:

```
NEXT_PUBLIC_API_BASE_URL = https://<your-railway-api-domain>.railway.app
```

### 3. Deploy

Click **Deploy**. Takes ~60 seconds. Vercel will automatically redeploy on every push to `master`.

### 4. Update CORS on Railway API

Once Vercel assigns a domain (e.g. `art-activity-collections.vercel.app`), update the Railway API env var:

```
API_ALLOWED_ORIGINS=https://art-activity-collections.vercel.app
```

Then redeploy the Railway API service.

### 5. Stop the Railway frontend service

Remove the Railway frontend deployment to stop billing (service config is preserved):

```bash
railway down --service frontend
```

Or via UI: Railway → frontend service → Deployments → three dots → **Remove**.

This stops CPU/Memory/Egress charges. The service remains in Railway and can be redeployed later if needed.

## Redeployment

Vercel automatically redeploys on every push to `master`. No manual steps needed after initial setup.

## Custom domain (optional)

In Vercel → Project → Settings → Domains, add your custom domain and follow the DNS instructions.

If you add a custom domain, update `API_ALLOWED_ORIGINS` in Railway to include it:

```
API_ALLOWED_ORIGINS=https://yourdomain.com,https://art-activity-collections.vercel.app
```

## When API moves to Cloud Run

Update `NEXT_PUBLIC_API_BASE_URL` in Vercel → Project → Settings → Environment Variables to the Cloud Run URL, then trigger a redeploy.
