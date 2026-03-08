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

The frontend reads `NEXT_PUBLIC_API_BASE_URL` (default `http://127.0.0.1:8000`).

## Railway
- Service root directory: `frontend`
- Build command: `npm ci && npm run build`
- Start command: `npm run start`
- Set `NEXT_PUBLIC_API_BASE_URL` to your API service public URL.
