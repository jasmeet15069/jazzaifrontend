import ast
import json
import operator
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import torch
from fastapi import FastAPI
from pydantic import BaseModel, Field
from transformers import GPT2LMHeadModel, GPT2TokenizerFast


MODEL_DIR = Path(os.getenv("MODEL_DIR", "/opt/texting-coding-model/model"))
COACH_KNOWLEDGE_FILE = Path(
    os.getenv(
        "COACH_KNOWLEDGE_FILE",
        "/opt/texting-coding-model/knowledge/advait_ai_knowledge.coach.json",
    )
)
DEFAULT_MAX_NEW_TOKENS = int(os.getenv("DEFAULT_MAX_NEW_TOKENS", "96"))
MAX_CONTEXT_CHARS = int(os.getenv("MAX_CONTEXT_CHARS", "3000"))

torch.set_num_threads(int(os.getenv("TORCH_NUM_THREADS", "2")))

load_started = time.time()
tokenizer = GPT2TokenizerFast.from_pretrained(
    MODEL_DIR,
    bos_token="<s>",
    eos_token="</s>",
    unk_token="<unk>",
    pad_token="<pad>",
    mask_token="<mask>",
)
model = GPT2LMHeadModel.from_pretrained(MODEL_DIR)
model.eval()
loaded_at = time.time()

app = FastAPI(title="Jazz AI V1.0 From Scratch LLM")


def _load_coach_knowledge(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8", errors="replace"))
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


class GenerateRequest(BaseModel):
    prompt: str = Field(..., min_length=1)
    max_new_tokens: int = Field(DEFAULT_MAX_NEW_TOKENS, ge=1, le=256)
    temperature: float = Field(0.0, ge=0.0, le=2.0)
    top_p: float = Field(0.9, ge=0.1, le=1.0)
    seed: Optional[int] = None


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def _word_hits(text: str, words: set) -> int:
    low = _norm(text)
    found = 0
    for word in words:
        if " " in word:
            found += 1 if word in low else 0
        else:
            found += 1 if re.search(rf"\b{re.escape(word)}\b", low) else 0
    return found


def _has_devanagari(text: str) -> bool:
    return bool(re.search(r"[\u0900-\u097f]", text or ""))


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
    text = _norm(user_text)
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
        result = _safe_eval_math(ast.parse(expr, mode="eval"))
    except ZeroDivisionError:
        return "Cannot divide by zero."
    except Exception:
        return None
    return f"{expr} = {_format_number(result)}"


def _parse_last_user(prompt: str) -> str:
    prompt = (prompt or "").strip()
    if len(prompt) > MAX_CONTEXT_CHARS:
        prompt = prompt[-MAX_CONTEXT_CHARS:]
    matches = list(re.finditer(r"(?ims)^User:\s*(.*?)(?=^(?:Assistant|System|User):|\Z)", prompt))
    if matches:
        return matches[-1].group(1).strip()
    return prompt


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


def _lines(title: str, options: List[str], reason: str, lang: str) -> str:
    numbered = "\n".join(f"{i}. {line}" for i, line in enumerate(options, 1))
    if lang == "hi":
        return f"{title}\n\n{numbered}\n\nKyu: {reason}"
    return f"{title}\n\n{numbered}\n\nWhy: {reason}"


def _coach_templates() -> Dict[str, Any]:
    return (COACH_KNOWLEDGE.get("templates") or {}) if isinstance(COACH_KNOWLEDGE, dict) else {}


def _router_answer(user_text: str) -> Optional[str]:
    lang = _detect_language(user_text)
    templates = _coach_templates()
    low = _norm(user_text)

    if re.fullmatch(r"(hi|hello|hey|yo|hii|hiii|namaste|namaskar)", low):
        if lang == "hi":
            return "Haan, main Jazz AI V1.0 hoon. Batao, kis cheez mein help chahiye?"
        return "Hi, I am Jazz AI V1.0. Tell me what you want to work on."

    if re.search(r"\b(who are you|what are you|your name|model are you|kaun ho|tum kaun)\b", low):
        if lang == "hi":
            return "Main Jazz AI V1.0 hoon, ek private from-scratch local model jisme Hindi/Hinglish, coding, tools, aur dating/texting coach routing added hai."
        return "I am Jazz AI V1.0, a private from-scratch local model with added routing for Hindi/Hinglish, coding, tools, and dating/texting coaching."

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
    return _lines(
        "Dating/texting game plan:",
        templates.get("generic_dating_hi") or [
            "Pehle boring question mat pooch. Curiosity hook ya light assumption se start kar.",
            "Uski profile/vibe se ek playful assumption bana.",
            "Reply aaye to 'why?' pooch aur uske answer par light tease kar.",
            "Energy low ho ya rejection ho to respectfully disengage.",
        ],
        "Curiosity -> assumption -> why -> light tease -> imagination game.",
        "hi" if lang == "hi" else "en",
    )


def _format_scratch_prompt(prompt: str, user_text: str) -> str:
    lang = _detect_language(user_text)
    instruction = (
        "System: You are Jazz AI V1.0, a from-scratch local assistant. "
        "Answer directly, briefly, and do not repeat role labels. "
    )
    if lang == "hi":
        instruction += "Use Hindi/Hinglish. "
    elif lang == "fr":
        instruction += "Use French. "
    return instruction + "\n" + prompt.strip() + "\nAssistant:"


def _clean_response(text: str, prompt: str) -> str:
    out = (text or "").strip()
    if out.startswith(prompt):
        out = out[len(prompt):].strip()
    out = re.sub(r"^(?:Assistant|assistant)\s*:\s*", "", out).strip()
    for marker in ("\nUser:", "\nAssistant:", "\nSystem:", "<|endoftext|>"):
        if marker in out:
            out = out.split(marker, 1)[0].strip()
    return out or "I am ready."


@app.get("/health")
def health():
    return {
        "status": "ok",
        "display_name": "Jazz AI V1.0",
        "internal_model_id": "jazz-ai-testing",
        "runtime": "from_scratch_gpt2",
        "model_dir": str(MODEL_DIR),
        "loaded": True,
        "load_seconds": round(loaded_at - load_started, 2),
        "coach_knowledge": bool(COACH_KNOWLEDGE),
        "calculator": True,
    }


@app.post("/generate")
def generate(request: GenerateRequest):
    if request.seed is not None:
        torch.manual_seed(request.seed)

    user_text = _parse_last_user(request.prompt)
    math_answer = _simple_math_answer(user_text)
    if math_answer:
        return {"text": math_answer, "model_id": "jazz-ai-testing", "source": "calculator"}

    routed = _router_answer(user_text)
    if routed:
        return {"text": routed, "model_id": "jazz-ai-testing", "source": "coach_router"}

    prompt = _format_scratch_prompt(request.prompt[-MAX_CONTEXT_CHARS:], user_text)
    inputs = tokenizer(prompt, return_tensors="pt", truncation=True, max_length=512)
    with torch.no_grad():
        outputs = model.generate(
            **inputs,
            max_new_tokens=min(request.max_new_tokens, 128),
            do_sample=request.temperature > 0.05,
            temperature=request.temperature if request.temperature > 0.05 else None,
            top_p=request.top_p,
            repetition_penalty=1.08,
            no_repeat_ngram_size=4,
            pad_token_id=tokenizer.pad_token_id or tokenizer.eos_token_id,
            eos_token_id=tokenizer.eos_token_id,
        )
    raw = tokenizer.decode(outputs[0], skip_special_tokens=True)
    return {"text": _clean_response(raw, prompt), "model_id": "jazz-ai-testing", "source": "scratch_model"}
