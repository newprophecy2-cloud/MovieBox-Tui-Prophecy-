# Hostinger KVM 4 deployment runbook

This is a deployment preparation guide, not evidence that the VPS has been configured.

1. Create a non-root service user and apply OS security updates. Permit only SSH, HTTP, and HTTPS at the firewall. Keep PostgreSQL and Redis bound to localhost or a private interface.
2. Install Rust or deploy a prebuilt release binary, plus FFmpeg and FFprobe. Verify `ffmpeg -version` and `ffprobe -version` before enabling playback.
3. Create a PostgreSQL database and least-privilege application role. Apply `migrations/001_catalog.sql` with a migration/deploy role, then restrict the runtime role to the application schema.
4. Install Redis locally with an explicit maxmemory and eviction policy. Redis is reconstructible cache/coordination state; it must never be the only location for catalog or pending import data.
5. Configure `/etc/prophecy/prophecy.env` from `.env.example`, using a strong random `ADMIN_TOKEN`, a private `DATABASE_URL`, and a local/private `REDIS_URL`. Do not commit this file.
6. Run the backend behind a TLS reverse proxy. Expose `/health` for liveness and `/ready` only after the database/readiness policy has been finalized for the deployment.
7. Run a manual database backup and restore drill before importing catalog data. Store encrypted backups outside the VPS and test restore to a separate database.
8. Monitor CPU, memory, network, active connections, PostgreSQL pool usage, Redis latency, FFmpeg process count, remux sessions, import job failures, and HTTP p95 latency. Tune concurrency only from measurements on the actual KVM 4 hardware.
9. Roll back by deploying the previous verified commit and rerunning `/health`, `/ready`, database connectivity, route-contract, and playback fixture checks. Never delete the database during rollback.

No production readiness or load capacity should be inferred from a successful local build alone.
