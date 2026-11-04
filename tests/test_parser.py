from pathlib import Path

import pytest

from app.parser import (
    ParseError, clean_line, find_secrets, is_valid_path, is_valid_rid,
    parse_line, parse_lines, redact_secrets,
)

GOOD = "2026-10-20T12:34:56Z INFO auth 192.0.2.10 POST /api/login 200 43ms rid=ab12cd34"
SAMPLE = Path(__file__).resolve().parent.parent / "data" / "sample.log"


# ---------- основная маска + валидация ----------
def test_valid_line_is_normalized():
    assert parse_line(GOOD) == {
        "timestamp": "2026-10-20T12:34:56Z", "level": "INFO", "service": "auth",
        "ip": "192.0.2.10", "method": "POST", "path": "/api/login",
        "status": 200, "duration_ms": 43, "rid": "ab12cd34",
    }


def test_valid_error_line_with_slow_request():
    r = parse_line("2026-10-20T12:34:58Z ERROR orders 198.51.100.8 GET /api/orders 503 742ms rid=ee44cc22")
    assert (r["level"], r["status"], r["duration_ms"]) == ("ERROR", 503, 742)


@pytest.mark.parametrize("bad, reason_part", [
    (GOOD.replace("2026-10-20", "2026-13-45"), "timestamp"),
    (GOOD.replace("192.0.2.10", "999.0.2.10"), "IPv4"),
    (GOOD.replace("INFO", "DEBUG"), "уровень"),
    (GOOD.replace("auth", "billing"), "сервис"),
    (GOOD.replace("POST", "PATCH"), "метод"),
    (GOOD.replace(" 200 ", " 999 "), "HTTP-код"),
    (GOOD.replace(" 200 ", " 099 "), "HTTP-код"),
    (GOOD.replace("43ms", "-5ms"), "отрицательная"),
    (GOOD.replace("43ms", "600001ms"), "длительность"),
    (GOOD.replace("rid=ab12cd34", "rid=XYZ"), "rid"),
    (GOOD.replace("rid=ab12cd34", "rid=AB12CD34"), "rid"),
    (GOOD.replace("/api/login", "api/login"), "путь"),
    (GOOD.replace("/api/login", "/api/../etc"), "путь"),
])
def test_invalid_field_gives_clear_reason(bad, reason_part):
    with pytest.raises(ParseError) as exc:
        parse_line(bad, line_no=7)
    assert exc.value.line_no == 7
    assert reason_part in exc.value.reason


@pytest.mark.parametrize("raw", ["", "   ", None, "это вообще не лог", GOOD + " extra"])
def test_empty_and_garbage_rejected(raw):
    with pytest.raises(ParseError):
        parse_line(raw)


def test_arabic_digits_are_not_digits():
    # re.ASCII: юникодные цифры не должны проходить как числа
    with pytest.raises(ParseError):
        parse_line(GOOD.replace("200", "٢٠٠"))


def test_parse_lines_keeps_line_numbers():
    res = parse_lines([GOOD, "мусор", GOOD.replace("auth", "nope")])
    assert len(res.valid) == 1
    assert [e.line_no for e in res.errors] == [2, 3]


# ---------- дополнительные регулярки: по тесту на каждую ----------
def test_regex_clean_line_whitespace_and_control_chars():
    dirty = "  2026-10-20T12:34:56Z\tINFO   auth 192.0.2.10 POST /api/login 200 43ms rid=ab12cd34\x00 \r"
    assert clean_line(dirty) == GOOD
    assert parse_line(dirty)["rid"] == "ab12cd34"


@pytest.mark.parametrize("rid, ok", [
    ("ab12cd34", True), ("00000000", True), ("ab12cd3", False),
    ("ab12cd345", False), ("ab12cd3g", False), ("AB12CD34", False),
])
def test_regex_rid(rid, ok):
    assert is_valid_rid(rid) is ok


@pytest.mark.parametrize("path, ok", [
    ("/", True), ("/api/orders/42", True), ("api/orders", False),
    ("/api/../secret", False), ("/api/<script>", False),
])
def test_regex_path(path, ok):
    assert is_valid_path(path) is ok


def test_regex_secrets_find_and_redact():
    text = "login ok password=hunter2 token: abc123 API_KEY=zzz"
    assert find_secrets(text) == ["password", "token", "api_key"]
    red = redact_secrets(text)
    assert "hunter2" not in red and "abc123" not in red and "zzz" not in red
    assert red.count("***") == 3


def test_error_does_not_leak_secrets():
    with pytest.raises(ParseError) as exc:
        parse_line("garbage password=supersecret")
    assert "supersecret" not in exc.value.line
    assert "supersecret" not in str(exc.value.to_dict())


# ---------- проверка data/sample.log по требованиям задания ----------
@pytest.fixture(scope="module")
def sample_result():
    lines = SAMPLE.read_text(encoding="utf-8").splitlines()
    return lines, parse_lines(lines)


def test_sample_has_enough_lines_and_errors(sample_result):
    lines, res = sample_result
    assert len(lines) >= 40
    assert len(res.errors) >= 5


def test_sample_covers_services_levels_statuses(sample_result):
    _, res = sample_result
    assert {r["service"] for r in res.valid} == {"auth", "orders", "payment"}
    assert {r["level"] for r in res.valid} == {"INFO", "WARN", "ERROR"}
    assert len({r["status"] for r in res.valid}) >= 4


def test_sample_valid_rids_unique(sample_result):
    _, res = sample_result
    rids = [r["rid"] for r in res.valid]
    assert len(rids) == len(set(rids))
