import os
import re
import time
import ast
import json
import operator
from pathlib import Path
from typing import Any, Dict, List, Optional

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
COACH_KNOWLEDGE_FILE = os.getenv(
    "COACH_KNOWLEDGE_FILE",
    str(Path(__file__).resolve().parent / "knowledge" / "advait_ai_knowledge.coach.json"),
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


def _load_coach_knowledge(path: str) -> Dict[str, Any]:
    file = Path(path)
    if not file.exists():
        return {}
    try:
        return json.loads(file.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        return {}


COACH_KNOWLEDGE = _load_coach_knowledge(COACH_KNOWLEDGE_FILE)

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


_HINDI_HINTS = {
    "hindi", "hinglish", "mai", "mein", "mujhe", "bata", "batao", "bol",
    "bolo", "kya", "kaise", "karu", "karun", "bheju", "bhejna", "ladki",
    "usne", "usko", "baat", "reply", "message", "pyaar", "pyar",
}
_FRENCH_HINTS = {"bonjour", "francais", "français", "parle", "merci", "salut"}
_SPANISH_HINTS = {"hola", "espanol", "español", "gracias", "habla"}
_GERMAN_HINTS = {"hallo", "deutsch", "danke", "sprich"}
_DATING_HINTS = {
    "dating", "date", "girl", "girls", "ladki", "crush", "flirt", "flirty",
    "text", "texting", "message", "dm", "whatsapp", "reply", "replied",
    "seen", "haha", "nice", "opener", "first message", "interested",
}
_REJECTION_HINTS = {
    "not interested", "no interest", "she rejected", "rejected", "reject",
    "mana kar diya", "interested nahi", "nahi interested", "not looking",
    "leave me", "stop texting",
}


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def _has_devanagari(text: str) -> bool:
    return bool(re.search(r"[\u0900-\u097f]", text or ""))


def _word_hits(text: str, words: set) -> int:
    low = _norm(text)
    found = 0
    for word in words:
        if " " in word:
            found += 1 if word in low else 0
        else:
            found += 1 if re.search(rf"\b{re.escape(word)}\b", low) else 0
    return found


def _detect_language(text: str) -> str:
    low = _norm(text)
    if _has_devanagari(text) or _word_hits(low, _HINDI_HINTS) >= 1:
        return "hi"
    if _word_hits(low, _FRENCH_HINTS) >= 1:
        return "fr"
    if _word_hits(low, _SPANISH_HINTS) >= 1:
        return "es"
    if _word_hits(low, _GERMAN_HINTS) >= 1:
        return "de"
    return "en" if re.search(r"[a-z]{3,}", low) else "hi"


def _is_language_switch(text: str) -> bool:
    low = _norm(text)
    return bool(
        re.search(r"\b(hindi|hinglish)\b.*\b(bol|bolo|baat|talk|speak|reply)\b", low)
        or re.search(r"\b(bol|bolo|baat|talk|speak|reply)\b.*\b(hindi|hinglish)\b", low)
        or "hindi mai bol" in low
        or "hindi mein baat" in low
    )


def _is_dating_intent(text: str) -> bool:
    return _word_hits(text, _DATING_HINTS) >= 1


def _is_rejection(text: str) -> bool:
    return _word_hits(text, _REJECTION_HINTS) >= 1


def _is_first_message(text: str) -> bool:
    low = _norm(text)
    return (
        "first message" in low
        or "opener" in low
        or "pehla message" in low
        or "kya bheju" in low
        or "message kya" in low
    )


def _is_haha_reply(text: str) -> bool:
    low = _norm(text)
    return any(x in low for x in ("haha nice", "she replied haha", "she said haha", "reply haha", "nice"))


def _coach_templates() -> Dict[str, Any]:
    return (COACH_KNOWLEDGE.get("templates") or {}) if isinstance(COACH_KNOWLEDGE, dict) else {}


def _lines(title: str, options: List[str], reason: str, lang: str) -> str:
    numbered = "\n".join(f"{i}. {line}" for i, line in enumerate(options, 1))
    if lang == "hi":
        return f"{title}\n\n{numbered}\n\nKyu: {reason}"
    return f"{title}\n\n{numbered}\n\nWhy: {reason}"


def _coach_direct_answer(user_text: str) -> Optional[str]:
    lang = _detect_language(user_text)
    templates = _coach_templates()

    if _is_language_switch(user_text):
        return templates.get(
            "language_ack_hi",
            "Haan, ab main Hindi/Hinglish mein baat karunga. Batao, kis cheez mein help chahiye?",
        )

    if lang == "fr" and not _is_dating_intent(user_text):
        return "Oui, je peux parler francais. Dis-moi ce que tu veux faire, et je te repondrai clairement."

    if lang == "es" and not _is_dating_intent(user_text):
        return "Si, puedo hablar espanol. Dime que necesitas y te respondo claro."

    if lang == "de" and not _is_dating_intent(user_text):
        return "Ja, ich kann Deutsch sprechen. Sag mir, wobei ich helfen soll."

    if not _is_dating_intent(user_text):
        return None

    if _is_rejection(user_text):
        if lang == "hi":
            return templates.get(
                "rejection_hi",
                "Agar usne clearly bola ki interested nahi hai, graceful close kar: 'All good, thanks for being honest. Take care.' Phir move on. Pressure ya chase mat kar.",
            )
        return templates.get(
            "rejection_en",
            "If she clearly says she is not interested, close respectfully: 'All good, thanks for being honest. Take care.' Then move on. Do not pressure or chase.",
        )

    if _is_first_message(user_text):
        options = templates.get("first_message_hi") or [
            "Teri vibe dekh ke lag raha hai tu sweet dikhti hai, par thodi trouble bhi hai.",
            "Wait, tu itni calm dikhti hai ya bas profile ka illusion hai?",
            "Tu woh type lagti hai jo innocent face bana ke sabse zyada chaos karti hai.",
        ]
        return _lines("Ye bhej:", options[:3], "Generic hi/hello boring hai. Light assumption curiosity create karti hai.", "hi")

    if _is_haha_reply(user_text):
        if lang == "hi":
            options = templates.get("haha_nice_hi") or [
                "Haha nice? Bas itna hi? Mujhe laga tumhare paas thoda better comeback hoga.",
                "Nice matlab impressed ho ya politely judge kar rahi ho?",
                "Theek hai, ab tumhari turn. Ek honest assumption mere baare mein.",
            ]
            return _lines("Reply options:", options[:3], "Uske low-effort reply ko playful challenge mein convert karo.", "hi")
        return _lines(
            "Reply options:",
            [
                "Haha nice? That's all? I expected a slightly better comeback from you.",
                "Nice as in impressed, or nice as in politely judging me?",
                "Okay, your turn. Make one honest assumption about me.",
            ],
            "Turn the low-effort reply into a playful challenge without chasing.",
            "en",
        )

    options = templates.get("generic_dating_hi") or [
        "Pehle boring question mat pooch. Curiosity hook ya light assumption se start kar.",
        "Uski profile/vibe se ek playful assumption bana.",
        "Reply aaye to 'why?' pooch aur uske answer par light tease kar.",
        "Energy low ho ya rejection ho to respectfully disengage.",
    ]
    return _lines("Dating/texting game plan:", options[:4], "Curiosity -> assumption -> why -> light tease -> imagination game.", "hi" if lang == "hi" else "en")


def _coach_system_context(user_text: str) -> str:
    lang = _detect_language(user_text)
    base = (
        "You are Jazz AI V1.0, a private Qwen-backed local LLM. "
        "Answer the current user directly. Default to Hindi/Hinglish when unclear. "
        "If the user clearly uses another language, reply in that same language. "
        "For dating or texting advice, be bold, playful, concise, and respectful. "
        "Never pressure after rejection; tell the user to disengage respectfully."
    )
    if _is_dating_intent(user_text) and COACH_KNOWLEDGE:
        rules = COACH_KNOWLEDGE.get("core_rules") or []
        safety = COACH_KNOWLEDGE.get("safety_rules") or []
        selected = "; ".join((rules[:5] + safety[:2]))
        base += " Coach rules: " + selected
    if lang == "hi":
        base += " Reply in natural Hindi/Hinglish."
    return base


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

    messages: List[dict] = []
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
    if not messages:
        messages.append({"role": "user", "content": prompt})
    last_user = ""
    for message in reversed(messages):
        if message.get("role") == "user":
            last_user = str(message.get("content") or "")
            break
    messages.insert(0, {"role": "system", "content": _coach_system_context(last_user)})
    return [messages[0], *messages[-8:]]


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
        "coach_knowledge": bool(COACH_KNOWLEDGE),
        "display_name": "Jazz AI V1.0",
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

    coach_answer = _coach_direct_answer(user_text)
    if coach_answer:
        return {
            "text": coach_answer,
            "model_id": MODEL_ID,
            "display_name": "Jazz AI V1.0",
            "source": "coach_router",
        }

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
