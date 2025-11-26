# Repository Guidelines

## Project Structure & Modules
- `golem-stats-backend/`: Django + Celery backend for stats.golem.network; Dockerfiles and Makefile for images.
|- `stats-backend/`: project root with apps: `api` (public endpoints), `api2` (newer endpoints/commands), `collector` (data ingestion, Celery tasks), `core` (settings/URLs/shared utils), `metamask`, `healthcheck`, `yapapi`, and `static/`.
- Tests: `stats-backend/api/tests` and `stats-backend/collector/tests`. Docker build contexts live in `dockerfiles/`.

## Build, Test, and Development Commands
- Install deps: `cd golem-stats-backend && pip install -r requirements.pip`.
- Run dev server: `cd stats-backend && python manage.py runserver 0.0.0.0:8000`.
- Database: `python manage.py migrate`; create superuser if needed with `python manage.py createsuperuser`.
- Celery: `celery -A core worker -l info` and `celery -A core beat -l info` (from `stats-backend`).
- Tests: `python manage.py test` (or target app: `python manage.py test api`).
- Docker: `make build-amd64` (builds Django/Celery/Beat/Yagna images) and `make push-amd64` (publish; login first with `make login`). Local stack: `docker-compose -f docker-compose-dev.yml up`.

## Coding Style & Naming Conventions
- PEP 8, 4-space indents; `snake_case` for modules/functions/vars, `PascalCase` for classes.
- Keep views and tasks small; move shared logic to `core/` helpers. Prefer DRF serializers for validation.
- Add type hints to new/modified code. Configure via env vars (no secrets in code); keep logging consistent with existing handlers.

## Testing Guidelines
- Add tests near the code they cover (app-level `tests/`). Name files `test_*.py` and functions `test_<behavior>`.
- Mock external services (Redis, Yagna, HTTP) to keep tests deterministic. Aim to cover model logic plus API responses.
- Quick commands: `python manage.py test api.tests.test_models` or `python manage.py test collector`.

## Commit & Pull Request Guidelines
- Commits: concise, imperative subjects (repo history shows short titles like “Update main.yml”; prefer explicit ones).
- PRs: state intent, key changes, and verification (`python manage.py test`, manual endpoint checks). Link issues; include sample API responses or screenshots for user-facing changes. Call out migrations or ops steps.

## Security & Configuration Tips
- Never commit wallet/key files (`key-*.json`, yagna artifacts); mount via volumes/env.
- Set `ALLOWED_HOSTS` appropriately; keep `DEBUG` off outside local dev. Ensure Redis/Postgres are reachable before starting workers to avoid reconnect churn.
