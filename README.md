# render-vless-node

VLESS over WebSocket Node service for Render free Web Service.

## Deploy on Render

- Create a free Web Service from this repository.
- Build command: npm install
- Start command: npm start
- Environment variables:
  - UUID: your VLESS UUID
  - WSPATH: your WebSocket path, e.g. /your-path
  - PORT: provided by Render (use the platform-injected PORT)

## Notes

- Render free Web Service: 750 hours/month, 512 MB, sleeps after 15 minutes without traffic, about 1 minute to wake.
- Client: VLESS + WS + TLS, port 443, path = WSPATH.
