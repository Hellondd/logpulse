"""Парсер строк лога LogPulse.

Формат строки:
2026-10-20T12:34:56Z INFO auth 192.0.2.10 POST /api/login 200 43ms rid=ab12cd34

Схема работы:
1. clean_line()  - чистим пробелы и управляющие символы (регулярки)
2. LINE_RE       - ОДНА основная маска с именованными группами, re.fullmatch
3. Семантика     - datetime, ipaddress, диапазоны, допустимые значения
Результат: нормализованный dict либо ParseError с причиной и номером строки.
"""
from __future__ import annotations

import ipaddress
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable

LEVELS = {"INFO", "WARN", "ERROR"}
SERVICES = {"auth", "orders", "payment"}
METHODS = {"GET", "POST", "PUT", "DELETE"}
MIN_STATUS, MAX_STATUS = 100, 599
MIN_DURATION_MS, MAX_DURATION_MS = 0, 600_000
SLOW_THRESHOLD_MS = 500
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

# re.ASCII, чтобы [0-9] не матчили юникодные цифры (например арабские).
_FLAGS = re.ASCII

# --- Основная маска: форма строки жёсткая, а "смысл" проверяем потом -------
# Так мы получаем понятные причины ошибок (а не просто "не подошло").
_FIELDS = [
    r"(?P<timestamp>[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z)",
    r"(?P<level>[A-Z]+)",
    r"(?P<service>[a-z]+)",
    r"(?P<ip>[0-9]{1,3}(?:\.[0-9]{1,3}){3})",
    r"(?P<method>[A-Z]+)",
    r"(?P<path>\S+)",
    r"(?P<status>[0-9]+)",
    r"(?P<duration_ms>-?[0-9]+)ms",
    r"rid=(?P<rid>\S+)",
]
LINE_RE = re.compile(" ".join(_FIELDS), _FLAGS)

# --- Дополнительные регулярки (>= 3, у каждой свой тест) -------------------
# 1) очистка: управляющие символы и лишние пробелы
CONTROL_RE = re.compile(r"[\x00-\x08\x0b-\x1f\x7f]")
WHITESPACE_RE = re.compile(r"[ \t]+")
# 2) rid: ровно 8 hex-символов в нижнем регистре
RID_RE = re.compile(r"[0-9a-f]{8}", _FLAGS)
# 3) путь: начинается с /, только безопасные символы, без ".."
PATH_RE = re.compile(r"/(?!.*\.\.)[A-Za-z0-9._~/-]*", _FLAGS)
# 4) поиск секретов в строке (password=..., token: ..., api_key=...)
SECRET_RE = re.compile(
    r"(?P<key>password|passwd|token|secret|api[_-]?key)(?P<sep>\s*[=:]\s*)(?P<val>\S+)",
    re.IGNORECASE,
)


class ParseError(ValueError):
    """Ошибка разбора: номер строки + причина + (очищенная от секретов) строка."""

    def __init__(self, line_no: int, reason: str, line: str = ""):
        self.line_no = line_no
        self.reason = reason
        self.line = redact_secrets(line)[:200]
        super().__init__(f"строка {line_no}: {reason}")

    def to_dict(self) -> dict:
        return {"line_no": self.line_no, "reason": self.reason, "line": self.line}


@dataclass
class ParseResult:
    valid: list[dict] = field(default_factory=list)
    errors: list[ParseError] = field(default_factory=list)


def clean_line(raw: str) -> str:
    """Убираем управляющие символы, схлопываем пробелы/табы, режем края."""
    no_ctrl = CONTROL_RE.sub("", raw)
    return WHITESPACE_RE.sub(" ", no_ctrl).strip()


def find_secrets(text: str) -> list[str]:
    """Ключи секретов, найденные в тексте (password, token ...)."""
    return [m.group("key").lower() for m in SECRET_RE.finditer(text)]


def redact_secrets(text: str) -> str:
    """Заменяем значения секретов на ***, чтобы не светить их в ошибках."""
    return SECRET_RE.sub(lambda m: f"{m.group('key')}{m.group('sep')}***", text)


def is_valid_rid(rid: str) -> bool:
    return RID_RE.fullmatch(rid) is not None


def is_valid_path(path: str) -> bool:
    return PATH_RE.fullmatch(path) is not None


def parse_line(raw: str | None, line_no: int = 1) -> dict:
    """Разбирает одну строку. Возвращает нормализованный dict или бросает ParseError."""
    if raw is None or not raw.strip():
        raise ParseError(line_no, "пустая строка", raw or "")

    line = clean_line(raw)
    m = LINE_RE.fullmatch(line)
    if m is None:
        raise ParseError(line_no, "строка не соответствует формату лога", line)
    f = m.groupdict()

    def fail(reason: str) -> ParseError:
        return ParseError(line_no, reason, line)

    # timestamp
    try:
        ts = datetime.strptime(f["timestamp"], TIMESTAMP_FORMAT)
    except ValueError:
        raise fail(f"некорректный timestamp: {f['timestamp']}") from None

    # допустимые значения
    if f["level"] not in LEVELS:
        raise fail(f"неподдерживаемый уровень: {f['level']}")
    if f["service"] not in SERVICES:
        raise fail(f"неизвестный сервис: {f['service']}")

    # IPv4
    try:
        ip = ipaddress.IPv4Address(f["ip"])
    except ipaddress.AddressValueError:
        raise fail(f"некорректный IPv4-адрес: {f['ip']}") from None

    if f["method"] not in METHODS:
        raise fail(f"неподдерживаемый HTTP-метод: {f['method']}")
    if not is_valid_path(f["path"]):
        raise fail(f"некорректный путь: {f['path']}")

    status = int(f["status"])
    if not MIN_STATUS <= status <= MAX_STATUS:
        raise fail(f"HTTP-код вне диапазона {MIN_STATUS}-{MAX_STATUS}: {status}")

    duration = int(f["duration_ms"])
    if duration < MIN_DURATION_MS:
        raise fail(f"отрицательная длительность: {duration}ms")
    if duration > MAX_DURATION_MS:
        raise fail(f"длительность больше {MAX_DURATION_MS}ms: {duration}ms")

    if not is_valid_rid(f["rid"]):
        raise fail("rid должен быть ровно 8 hex-символов (0-9a-f)")

    return {
        "timestamp": ts.strftime(TIMESTAMP_FORMAT),
        "level": f["level"],
        "service": f["service"],
        "ip": str(ip),
        "method": f["method"],
        "path": f["path"],
        "status": status,
        "duration_ms": duration,
        "rid": f["rid"],
    }


def parse_lines(lines: Iterable[str]) -> ParseResult:
    """Разбирает пачку строк; номера строк начинаются с 1."""
    result = ParseResult()
    for no, raw in enumerate(lines, start=1):
        try:
            result.valid.append(parse_line(raw, no))
        except ParseError as err:
            result.errors.append(err)
    return result
