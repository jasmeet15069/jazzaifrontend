#!/usr/bin/env bash
set -euo pipefail

export LD_LIBRARY_PATH=/root/local-llm/llama-b8833

exec /root/local-llm/llama-b8833/llama-server \
  -m /root/local-llm/models/Dolphin3.0-Qwen2.5-0.5B-Q4_K_M.gguf \
  --host 127.0.0.1 \
  --port 8012 \
  -c 768 \
  -t 1 \
  -tb 1 \
  -np 1 \
  -n 160 \
  -b 256 \
  -ub 128 \
  --cache-reuse 64 \
  --no-webui
