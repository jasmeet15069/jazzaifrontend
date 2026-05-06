import os
import re
import time
import ast
import operator
from pathlib import Path
from typing import List, Optional

import torch
from fastapi import FastAPI
from pydantic import BaseModel, Field
from transformers import AutoModelForCausalLM, AutoTokenizer


MODEL_ID = os.getenv("MODEL_ID", "Qwen/Qwen2.5-0.5B-Instruct")
MAX_CONTEXT_CHARS = int(os.getenv("MAX_CONTEXT_CHARS", "6000"))
DEFAULT_MAX_NEW_TOKENS = int(os.getenv("DEFAULT_MAX_NEW_TOKENS", "96"))
TRAINING_EXAMPLES_FILE = os.getenv(
    "TRAINING_EXAMPLES_FILE",
    "/home/azureuser/llm-0_5b-from-scratch/sample_data/texting_coding_train.txt",
)

torch.set_num_threads(int(os.getenv("TORCH_NUM_THREADS", "2")))

app = FastAPI(title="JAZZ AI Testing Local LLM")

load_started = time.time()
tokenizer = AutoTokenizer.from_pretrained(MODEL_ID)
model = AutoModelForCausalLM.from_pretrained(
    MODEL_ID,
    dtype=torch.float32,
    low_cpu_mem_usage=True,
)
model.eval()
loaded_at = time.time()


def _tokens(text: str) -> set:
    return {tok for tok in re.findall(r"[a-z0-9_]+", (text or "").lower()) if len(tok) > 2}


def _load_training_examples(path: str) -> List[dict]:
    file = Path(path)
    if not file.exists():
        return []
    raw = file.read_text(encoding="utf-8", errors="replace")
    examples: List[dict] = []
    for chunk in re.split(r"\n\s*\n(?=User:)", raw):
        match = re.match(r"\s*User:\s*(.*?)\nAssistant:\s*(.*?)\s*$", chunk, re.S)
        if not match:
            continue
        user = match.group(1).strip()
        answer = match.group(2).strip().replace("\\n", "\n")
        examples.append({"user": user, "answer": answer, "tokens": _tokens(user)})
    return examples


TRAINING_EXAMPLES = _load_training_examples(TRAINING_EXAMPLES_FILE)

_BIN_OPS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
}
_UNARY_OPS = {ast.UAdd: operator.pos, ast.USub: operator.neg}


def _safe_eval_math(node):
    if isinstance(node, ast.Expression):
        return _safe_eval_math(node.body)
    if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
        return node.value
    if isinstance(node, ast.UnaryOp) and type(node.op) in _UNARY_OPS:
        return _UNARY_OPS[type(node.op)](_safe_eval_math(node.operand))
    if isinstance(node, ast.BinOp) and type(node.op) in _BIN_OPS:
        left = _safe_eval_math(node.left)
        right = _safe_eval_math(node.right)
        if isinstance(node.op, ast.Pow) and abs(right) > 12:
            raise ValueError("Exponent too large")
        if isinstance(node.op, (ast.Div, ast.FloorDiv, ast.Mod)) and right == 0:
            raise ZeroDivisionError("division by zero")
        return _BIN_OPS[type(node.op)](left, right)
    raise ValueError("Unsupported expression")


def _format_number(value) -> str:
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    if isinstance(value, float):
        return f"{value:.10g}"
    return str(value)


def _simple_math_answer(user_text: str) -> Optional[str]:
    text = (user_text or "").strip().lower()
    text = text.replace("×", "*").replace("÷", "/").replace("−", "-")
    text = re.sub(r"\b(what\s+is|what's|calculate|compute|solve|please|answer|equals|equal\s+to)\b", " ", text)
    text = text.replace("?", " ").replace("=", " ")
    text = re.sub(r"\bplus\b", "+", text)
    text = re.sub(r"\bminus\b", "-", text)
    text = re.sub(r"\btimes\b|\bmultiplied\s+by\b", "*", text)
    text = re.sub(r"\bdivided\s+by\b", "/", text)
    expr = re.sub(r"\s+", " ", text).strip()
    if not re.fullmatch(r"[0-9\s\.\+\-\*\/%\(\)]{3,}", expr):
        return None
    if not re.search(r"[0-9]\s*[\+\-\*\/%]\s*[0-9]", expr):
        return None
    try:
        tree = ast.parse(expr, mode="eval")
        result = _safe_eval_math(tree)
    except Exception as exc:
        if isinstance(exc, ZeroDivisionError):
            return "Cannot divide by zero."
        return None
    return f"{expr} = {_format_number(result)}"


class GenerateRequest(BaseModel):
    prompt: str = Field(..., min_length=1)
    max_new_tokens: int = Field(DEFAULT_MAX_NEW_TOKENS, ge=1, le=256)
    temperature: float = Field(0.0, ge=0.0, le=2.0)
    top_p: float = Field(0.9, ge=0.1, le=1.0)
    seed: Optional[int] = None


def _parse_legacy_prompt(prompt: str) -> List[dict]:
    prompt = (prompt or "").strip()
    if len(prompt) > MAX_CONTEXT_CHARS:
        prompt = prompt[-MAX_CONTEXT_CHARS:]

    messages: List[dict] = [
        {
            "role": "system",
            "content": (
                "You are jazz-ai-testing, a concise general AI assistant for coding, "
                "writing, analysis, and everyday help. JAZZ AI is the product name, "
                "not a jazz-music context. Answer the current user directly. If the "
                "user asks you to write, create, generate, or convert something, "
                "output the requested artifact instead of explaining how to do it. "
                "For code, provide correct working code. Follow exact output "
                "constraints. Do not repeat role labels or training examples."
            ),
        }
    ]
    current_role = None
    current_lines: List[str] = []

    def flush() -> None:
        nonlocal current_role, current_lines
        if current_role and current_lines:
            content = "\n".join(current_lines).strip()
            if content:
                messages.append({"role": current_role, "content": content})
        current_role = None
        current_lines = []

    for raw_line in prompt.splitlines():
        line = raw_line.rstrip()
        match = re.match(r"^(System|User|Assistant):\s*(.*)$", line, re.I)
        if match:
            flush()
            role_name = match.group(1).lower()
            current_role = "assistant" if role_name == "assistant" else role_name
            tail = match.group(2).strip()
            if tail:
                current_lines.append(tail)
            continue
        if current_role:
            current_lines.append(line)

    flush()
    if messages and messages[-1]["role"] == "assistant":
        messages.pop()
    if len(messages) == 1:
        messages.append({"role": "user", "content": prompt})
    return messages[-9:]


def _last_user_text(prompt: str) -> str:
    messages = _parse_legacy_prompt(prompt)
    for message in reversed(messages):
        if message.get("role") == "user":
            return str(message.get("content") or "")
    return prompt


def _training_example_answer(user_text: str) -> Optional[str]:
    query = _tokens(user_text)
    if len(query) < 3:
        return None
    best_score = 0.0
    best_answer: Optional[str] = None
    for example in TRAINING_EXAMPLES:
        candidate = example["tokens"]
        if not candidate:
            continue
        overlap = len(query & candidate)
        score = overlap / max(1, len(query | candidate))
        if overlap >= 3 and score > best_score:
            best_score = score
            best_answer = example["answer"]
    return best_answer if best_score >= 0.38 else None


def _format_prompt(prompt: str) -> str:
    messages = _parse_legacy_prompt(prompt)
    if getattr(tokenizer, "chat_template", None):
        return tokenizer.apply_chat_template(
            messages,
            tokenize=False,
            add_generation_prompt=True,
        )
    body = "\n".join(f"{m['role'].title()}: {m['content']}" for m in messages if m["role"] != "system")
    return body + "\nAssistant:"


def _clean_response(text: str) -> str:
    out = (text or "").strip()
    out = re.sub(r"^(?:Assistant|assistant)\s*:\s*", "", out).strip()
    for marker in ("\nUser:", "\nAssistant:", "\nSystem:", "<|im_end|>", "<|endoftext|>"):
        if marker in out:
            out = out.split(marker, 1)[0].strip()
    return out or "I am ready."


@app.get("/health")
def health():
    return {
        "status": "ok",
        "model_id": MODEL_ID,
        "loaded": True,
        "load_seconds": round(loaded_at - load_started, 2),
        "training_examples": len(TRAINING_EXAMPLES),
        "calculator": True,
    }


@app.post("/generate")
def generate(request: GenerateRequest):
    if request.seed is not None:
        torch.manual_seed(request.seed)

    user_text = _last_user_text(request.prompt)
    math_answer = _simple_math_answer(user_text)
    if math_answer:
        return {"text": math_answer, "model_id": MODEL_ID, "source": "calculator"}

    example_answer = _training_example_answer(user_text)
    if example_answer:
        return {
            "text": example_answer,
            "model_id": MODEL_ID,
            "source": "training_example",
        }

    prompt = _format_prompt(request.prompt)
    inputs = tokenizer(prompt, return_tensors="pt", truncation=True, max_length=1024)
    generation_kwargs = {
        **inputs,
        "max_new_tokens": min(request.max_new_tokens, 128),
        "repetition_penalty": 1.08,
        "no_repeat_ngram_size": 4,
        "pad_token_id": tokenizer.eos_token_id,
        "eos_token_id": tokenizer.eos_token_id,
    }
    if request.temperature <= 0.05:
        generation_kwargs["do_sample"] = False
    else:
        generation_kwargs.update(
            {
                "do_sample": True,
                "temperature": request.temperature,
                "top_p": request.top_p,
            }
        )

    with torch.no_grad():
        outputs = model.generate(**generation_kwargs)

    generated = tokenizer.decode(
        outputs[0][inputs["input_ids"].shape[1] :],
        skip_special_tokens=True,
    )
    return {"text": _clean_response(generated), "model_id": MODEL_ID}
