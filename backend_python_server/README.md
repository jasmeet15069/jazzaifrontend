# JAZZ AI Backend Python Server

This folder contains the FastAPI backend used by the live JAZZ AI deployment.

## Files

- `server14.py` - main backend server
- `.env.example` - safe placeholder environment file
- `.gitignore` - excludes real secrets, local databases, uploads, logs, and runtime folders

## Run

Create a real `.env` from `.env.example`, fill the required keys, then run:

```bash
python server14.py
```

The live SSH deployment still uses `/root/jazzai/server14.py` on port `8000`. Do not commit a real `.env`.
