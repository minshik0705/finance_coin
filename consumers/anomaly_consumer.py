# consumers/anomaly_consumer.py
from __future__ import annotations

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

import time
from datetime import datetime, timezone

from anomaly_detect import run_all

# ────────────────────────────────────────
# 설정
# ────────────────────────────────────────

DETECT_INTERVAL_SECONDS  = 60    # 1분마다 탐지
RETRAIN_INTERVAL_MINUTES = 60    # 60분마다 재학습
EXCHANGES = ["binance", "okx", "bybit"]


# ────────────────────────────────────────
# 메인 루프
# ────────────────────────────────────────

def run():
    print("[INFO] anomaly_consumer 시작")
    print(f"[INFO] 탐지 주기:   {DETECT_INTERVAL_SECONDS}초")
    print(f"[INFO] 재학습 주기: {RETRAIN_INTERVAL_MINUTES}분")

    last_retrain_minute: int | None = None

    while True:
        try:
            now        = datetime.now(tz=timezone.utc)
            now_minute = now.hour * 60 + now.minute

            # 처음 실행이거나 재학습 주기가 됐을 때
            should_retrain = (
                last_retrain_minute is None
                or (now_minute - last_retrain_minute) % RETRAIN_INTERVAL_MINUTES == 0
            )

            if should_retrain:
                print(f"\n[INFO] === {now.strftime('%H:%M')} "
                      f"모델 재학습 포함 탐지 ===")
                last_retrain_minute = now_minute
            else:
                print(f"\n[INFO] === {now.strftime('%H:%M')} 탐지 실행 ===")

            for exchange in EXCHANGES:
                try:
                    run_all(exchange=exchange, retrain=should_retrain)
                except Exception as e:
                    print(f"[ERROR] {exchange} 탐지 실패: {e}")

        except KeyboardInterrupt:
            print("\n[INFO] anomaly_consumer 종료")
            break
        except Exception as e:
            print(f"[ERROR] 메인 루프 오류: {e}")

        print(f"[INFO] {DETECT_INTERVAL_SECONDS}초 후 다음 탐지...")
        time.sleep(DETECT_INTERVAL_SECONDS)


if __name__ == "__main__":
    run()
