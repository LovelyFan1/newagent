import json
import time

import requests


def main() -> None:
    url = "http://127.0.0.1:8000/api/v1/agent/query"
    payload = {"question": "对比比亚迪和长城汽车"}
    start = time.perf_counter()
    try:
        resp = requests.post(url, json=payload, timeout=30)
        elapsed = time.perf_counter() - start
        body = {}
        try:
            body = resp.json()
        except Exception:
            body = {"raw_text": resp.text[:500]}
        print(
            json.dumps(
                {
                    "ok": True,
                    "http_status": resp.status_code,
                    "elapsed_s": round(elapsed, 3),
                    "body": body,
                },
                ensure_ascii=False,
            )
        )
    except requests.Timeout as e:
        elapsed = time.perf_counter() - start
        print(
            json.dumps(
                {
                    "ok": False,
                    "timeout": True,
                    "elapsed_s": round(elapsed, 3),
                    "error": str(e),
                },
                ensure_ascii=False,
            )
        )
    except Exception as e:
        elapsed = time.perf_counter() - start
        print(
            json.dumps(
                {
                    "ok": False,
                    "timeout": False,
                    "elapsed_s": round(elapsed, 3),
                    "error": str(e),
                },
                ensure_ascii=False,
            )
        )


if __name__ == "__main__":
    main()
