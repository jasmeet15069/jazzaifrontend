# jazzaifrontend

Classic Jazz AI frontend and backend deployment bundle.

## CI/CD

GitHub Actions now runs on every push and pull request:

- Parses `index.html` inline JavaScript.
- Compiles `backend_python_server/server14.py`.
- Verifies backend secrets are not committed.

Backend deploy is optional and stays disabled until these GitHub repository secrets are added:

- `JAZZ_ENABLE_BACKEND_DEPLOY=true`
- `JAZZ_SERVER_HOST`
- `JAZZ_SERVER_USER`
- `JAZZ_SSH_PRIVATE_KEY`
- `JAZZ_SERVER_PATH` such as `/root/jazzai`
- `JAZZ_SERVER_PYTHON` such as `/root/jazzai/venv/bin/python`
