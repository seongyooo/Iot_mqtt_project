# cctv_test.py
# Sub 장비(민준_노트북 / 192.168.0.28)에서 실행

import time, json, csv
import paho.mqtt.client as mqtt
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional

SUB_BROKER  = "192.168.0.29"  # B4
EVENT_TOTAL = 100              # Pub 1대당 발송 수
CAM_LIST    = ["pi1", "pi2"]   # 카메라 목록

WAIT_AFTER_FAULT   = 20
WAIT_AFTER_RECOVER = 10

SCENARIOS = [
    ("정상",       []),
    ("단일-B1",    ["B1"]),
    ("단일-B2",    ["B2"]),
    ("단일-B3",    ["B3"]),
    ("단일-B4",    ["B4"]),
    ("이중-B1+B3", ["B1", "B3"]),
    ("이중-B1+B4", ["B1", "B4"]),
    ("이중-B2+B3", ["B2", "B3"]),
    ("이중-B2+B4", ["B2", "B4"]),
]

BROKER_INFO = {
    "B1": "민준_pi     192.168.0.7  (Linux)  → sudo systemctl stop mosquitto",
    "B2": "선교_노트북 192.168.0.11 (Windows)→ net stop mosquitto",
    "B3": "시진_노트북 192.168.0.13 (Windows)→ net stop mosquitto",
    "B4": "민준_pi     192.168.0.29 (Linux)  → sudo systemctl stop mosquitto",
}

# ─────────────────────────────────────────
# 수신기
# ─────────────────────────────────────────
@dataclass
class State:
    # cam_id → {seq: 수신시각} 으로 저장
    seqs:         dict = field(default_factory=dict)
    disconnect_t: Optional[float] = None
    reconnect_t:  Optional[float] = None
    connected:    bool = False

def make_receiver(state: State):
    """
    브로커에 구독자로 참여.
    subscriber_ws.c와 동시에 같은 메시지를 수신함.
    브로커가 두 구독자 모두에게 복사 전달하므로 기존 동작 무관.
    """
    def on_connect(c, ud, flags, rc):
        if rc == 0:
            state.connected = True
            if state.disconnect_t and not state.reconnect_t:
                state.reconnect_t = time.time()
                fo = state.reconnect_t - state.disconnect_t
                print(f"  [SUB 재연결] Failover = {fo:.2f}초  "
                      f"{datetime.now().strftime('%H:%M:%S')}")
            else:
                print(f"  [SUB 연결] {SUB_BROKER}  "
                      f"{datetime.now().strftime('%H:%M:%S')}")
            # subscriber_ws.c와 동일한 토픽 구독
            c.subscribe("camera/+/event", qos=2)

    def on_disconnect(c, ud, rc):
        state.connected = False
        if not state.disconnect_t:
            state.disconnect_t = time.time()
            print(f"  [SUB 끊김]  {datetime.now().strftime('%H:%M:%S')}")

    def on_message(c, ud, msg):
        """
        브로커로부터 메시지 수신.
        이 콜백은 ev_sender.py가 발행한 이벤트만 처리.
        (camera/+/event 토픽만 구독 중이므로 frame 메시지는 수신 안 함)
        """
        try:
            d   = json.loads(msg.payload)
            cam = d.get("cam", "unknown")
            seq = d.get("seq")
            ts  = d.get("ts", time.time())

            if seq is None:
                return

            state.seqs.setdefault(cam, {})
            state.seqs[cam][seq] = time.time()

            total = sum(len(v) for v in state.seqs.values())
            print(f"  [수신] cam={cam}  seq={seq:03d}  "
                  f"누계={total}  "
                  f"지연={time.time()-ts:.3f}s",
                  flush=True)
        except Exception as e:
            print(f"  [수신 오류] {e}")

    c = mqtt.Client(client_id="test_receiver")
    c.on_connect    = on_connect
    c.on_disconnect = on_disconnect
    c.on_message    = on_message
    c.connect_async(SUB_BROKER, 1883, 60)
    c.loop_start()
    return c

# ─────────────────────────────────────────
# 결과 계산
# ─────────────────────────────────────────
def calc_result(name, state, expected):
    total_recv = sum(len(v) for v in state.seqs.values())
    loss_rate  = round((1 - total_recv / expected) * 100, 1) \
                 if expected > 0 else 100.0

    # 카메라별 유실 seq 찾기
    missing_detail = []
    for cam in CAM_LIST:
        seqs = set(state.seqs.get(cam, {}).keys())
        if not seqs:
            missing_detail.append(f"{cam}: 전체 유실")
            continue
        full    = set(range(max(seqs) + 1))
        missing = sorted(full - seqs)
        if missing:
            missing_detail.append(f"{cam}:{missing[:10]}")

    fo = round(state.reconnect_t - state.disconnect_t, 2) \
         if state.disconnect_t and state.reconnect_t else None

    return {
        "시나리오":     name,
        "예상수신":     expected,
        "실제수신":     total_recv,
        "유실률(%)":    loss_rate,
        "failover(초)": fo if fo else "-",
        "유실seq":      " | ".join(missing_detail) if missing_detail else "없음",
        "시각":         datetime.now().strftime("%H:%M:%S"),
    }

# ─────────────────────────────────────────
# 시나리오 실행
# ─────────────────────────────────────────
def run_scenario(name, kill_list):
    expected = EVENT_TOTAL * len(CAM_LIST)

    print(f"\n{'='*55}")
    print(f"  시나리오: [{name}]")
    if kill_list:
        print(f"  장애 대상:")
        for b in kill_list:
            print(f"    {b}: {BROKER_INFO[b]}")
    print(f"{'='*55}")

    input("\n  시스템 정상 확인 후 Enter ▶ ")

    # 수신기 시작 (브로커에 구독자로 참여)
    state  = State()
    client = make_receiver(state)
    time.sleep(2)

    # Pub 발송 안내
    print(f"\n  ── 이벤트 발송 안내 ──")
    print(f"  Pub1(192.168.0.8) 터미널에서:")
    print(f"    python3 ev_sender.py 192.168.0.7 pi1 {EVENT_TOTAL} 0.3")
    print(f"  Pub2(192.168.0.9) 터미널에서:")
    print(f"    python3 ev_sender.py 192.168.0.7 pi2 {EVENT_TOTAL} 0.3")
    input("\n  두 Pub 발송 시작 후 Enter ▶ ")
    print(f"  발송 시작: {datetime.now().strftime('%H:%M:%S')}")

    # 장애 주입
    if kill_list:
        print(f"\n  3초 후 아래 브로커 중지하세요:")
        for b in kill_list:
            print(f"    {b}: {BROKER_INFO[b]}")
        time.sleep(3)
        input(f"  중지 완료 후 Enter ▶ ")
        print(f"  장애 시각: {datetime.now().strftime('%H:%M:%S')}")

    # 발송 완료 대기
    input(f"\n  두 Pub [완료] 출력 확인 후 Enter ▶ ")
    print(f"  {WAIT_AFTER_FAULT}초 대기 (SQLite flush 포함)...")
    time.sleep(WAIT_AFTER_FAULT)

    # 복구
    if kill_list:
        print(f"\n  ── 복구 ──")
        for b in kill_list:
            print(f"    {b}: mosquitto start")
        input(f"  시작 완료 후 Enter ▶ ")
        print(f"  복구 시각: {datetime.now().strftime('%H:%M:%S')}")
        print(f"  {WAIT_AFTER_RECOVER}초 안정화...")
        time.sleep(WAIT_AFTER_RECOVER)

    client.loop_stop()
    client.disconnect()

    # 결과 출력
    result = calc_result(name, state, expected)
    print(f"\n  ┌── 결과 {'─'*35}")
    print(f"  │  예상: {result['예상수신']}  "
          f"수신: {result['실제수신']}  "
          f"유실률: {result['유실률(%)']}%")
    print(f"  │  Failover:  {result['failover(초)']}초")
    print(f"  │  유실 seq:  {result['유실seq']}")
    print(f"  └{'─'*42}")

    return result

# ─────────────────────────────────────────
# 메인
# ─────────────────────────────────────────
def main():
    print("=" * 55)
    print("  CCTV Fault Tolerance Test")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)
    print("\n  [사전 준비]")
    print("  ev_sender.py를 Pub1, Pub2에 복사:")
    print("    scp ev_sender.py user@192.168.0.8:~/")
    print("    scp ev_sender.py user@192.168.0.9:~/")
    input("\n  복사 완료 후 Enter ▶ ")

    all_results = []
    for name, kill_list in SCENARIOS:
        r = run_scenario(name, kill_list)
        all_results.append(r)
        input(f"\n  시스템 정상 복구 확인 후 Enter (다음 시나리오) ▶ ")

    # CSV 저장
    fname = f"result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    with open(fname, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=all_results[0].keys())
        w.writeheader()
        w.writerows(all_results)

    # 최종 요약
    print(f"\n{'='*55}")
    print(f"  최종 결과 요약")
    print(f"{'='*55}")
    print(f"  {'시나리오':<14} {'수신률':>7} {'Failover':>10}")
    print(f"  {'-'*38}")
    for r in all_results:
        rate = f"{100 - r['유실률(%)']:.1f}%"
        fo   = f"{r['failover(초)']}초" if r['failover(초)'] != '-' else "—"
        print(f"  {r['시나리오']:<14} {rate:>7} {fo:>10}")
    print(f"\n  결과 저장 → {fname}")

if __name__ == "__main__":
    main()