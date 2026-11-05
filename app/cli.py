"""Маленькая утилита для ручной проверки без API.

  python -m app.cli load data/sample.log   # отправить файл в очередь
  python -m app.cli stats                  # показать статистику из Redis
  python -m app.cli reset                  # удалить ТОЛЬКО ключи logpulse:*
"""
import argparse
import json

from .redis_store import ALL_KEYS, get_redis, get_stats
from .service import ingest_lines


def main() -> None:
    p = argparse.ArgumentParser(prog="python -m app.cli")
    sub = p.add_subparsers(dest="cmd", required=True)
    load = sub.add_parser("load")
    load.add_argument("path")
    sub.add_parser("stats")
    sub.add_parser("reset")
    args = p.parse_args()

    r = get_redis()
    if args.cmd == "load":
        with open(args.path, encoding="utf-8") as fh:
            out = ingest_lines(r, fh.read().splitlines())
    elif args.cmd == "stats":
        out = get_stats(r)
    else:
        out = {"deleted_keys": r.delete(*ALL_KEYS)}
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
