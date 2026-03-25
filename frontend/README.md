# Frontend

Next.js frontend for browsing activities from the FastAPI backend.

## Run
1. Install dependencies:
   - `npm install`
2. Create env file:
   - `cp .env.local.example .env.local`
3. Start dev server:
   - `npm run dev`
4. Open:
   - `http://localhost:3000`

The frontend reads `NEXT_PUBLIC_API_BASE_URL` and defaults to same-origin `/api` when unset.

## Railway
- Service root directory: `frontend`
- Build command: `npm ci && npm run build`
- Start command: `npm run start`
- If the frontend and API are served behind the same public host, leave `NEXT_PUBLIC_API_BASE_URL` unset.
- If the API is deployed as a separate Railway service/domain, set `NEXT_PUBLIC_API_BASE_URL` to that public API URL.
