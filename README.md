# IoT MQTT CCTV 모니터링 시스템

MQTT와 Raspberry Pi 카메라를 이용한 내결함성 CCTV 모니터링 시스템.  
퍼블리셔가 MJPEG 프레임을 계층형 브로커 네트워크를 통해 전달하면, 브라우저 대시보드에서 실시간 영상과 네트워크 토폴로지를 확인할 수 있다.

---

## 아키텍처

```
<img width="1632" height="1088" alt="image" src="https://github.com/user-attachments/assets/cc633367-0ad4-4ba2-aed1-70e8f6ea5a79" />

```

- **Edge 브로커 (B1, B2):** 퍼블리셔가 연결하는 브로커. MQTT 브릿지를 통해 Core 브로커 양쪽으로 메시지를 전달한다.
- **Core 브로커 (B3, B4):** 서브스크라이버가 연결하는 브로커. Edge로부터 포워딩된 모든 트래픽을 수신한다.
- **Publisher (Pub1, Pub2):** 각 Raspberry Pi에서 실행. MJPEG 영상을 캡처해 프레임, 모션 이벤트, 상태 메시지를 발행한다. 연결된 Edge 브로커가 끊기면 자동으로 다른 브로커로 페일오버한다.
- **Subscriber:** Core 브로커에 연결해 수신한 데이터를 WebSocket으로 브라우저에 전달한다. Core 브로커 장애 시에도 페일오버를 수행한다.

### MQTT 토픽 구조

```
camera/<cam_id>/frame    QoS 0  non-retained  JPEG 바이너리
camera/<cam_id>/event    QoS 2  non-retained  JSON 모션 이벤트
camera/<cam_id>/status   QoS 1  retained      "online" | "offline"
```

Edge 브로커에서 Core 브로커로 브릿지될 때 토픽 앞에 경로 정보가 추가된다.

```
<edge>/<core>/camera/<cam_id>/<type>
예)  b1/b3/camera/pi1/frame
```

서브스크라이버는 이 경로 정보를 파싱해 어느 브로커가 트래픽을 전달하고 있는지 추적하고, 경로 변경을 브라우저에 알린다.

---

## 파일 구성

| 파일 | 설명 |
|---|---|
| `publisher.c` | 카메라 캡처 및 MQTT 발행, 브로커 페일오버 처리 |
| `subscriber_ws.c` | MQTT 구독 후 WebSocket으로 브라우저에 데이터 릴레이 |
| `index.html` | 브라우저 대시보드: 영상 그리드, 이벤트 로그, 네트워크 다이어그램 |
| `web_server.py` | `index.html` 제공용 경량 HTTP 서버 |
| `test_sub.c` | 메시지 유실률 및 페일오버 시간 측정용 테스트 서브스크라이버 |
| `Makefile` | publisher, subscriber_ws 빌드 규칙 |

---

## 빌드

### 의존성

- `libmosquitto` (mosquitto 개발 라이브러리)
- `libws` (C WebSocket 서버 라이브러리, subscriber_ws 전용)
- `libpthread`

```bash
# Debian/Ubuntu
sudo apt install libmosquitto-dev
```

### 컴파일

```bash
make
```

`publisher`와 `subscriber_ws` 두 바이너리가 생성된다.

---

## 설정

### Publisher (`publisher.c`)

| 상수 | 기본값 | 설명 |
|---|---|---|
| `CAM_ID` | `"pi1"` | 카메라 식별자. 장비마다 다르게 설정 |
| `BROKERS[]` | B1, B2 | 연결할 Edge 브로커 IP 및 포트 목록 |
| `CAM_WIDTH` / `CAM_HEIGHT` | 640x480 | 캡처 해상도 |
| `CAM_FPS` | 15 | 캡처 프레임레이트 |
| `CAM_QUALITY` | 80 | JPEG 품질 (0-100) |
| `MOTION_RATIO_THRESHOLD` | 1.20 | 모션 판정 프레임 크기 비율 임계값 |
| `MOTION_CONFIRM_FRAMES` | 3 | 모션 확정에 필요한 연속 프레임 수 |
| `CONNECT_TIMEOUT_SEC` | 3 | TCP 핸드셰이크 타임아웃 (초) |

두 번째 Raspberry Pi에서는 `CAM_ID`를 `"pi2"`로 변경한다.

### Subscriber (`subscriber_ws.c`)

| 상수 | 기본값 | 설명 |
|---|---|---|
| `BROKERS[]` | B3, B4 | 연결할 Core 브로커 IP 및 포트 목록 |
| `WS_PORT` | 8765 | WebSocket 서버 포트 |
| `BROKER_TIMEOUT_SEC` | 5 | 이 시간 동안 수신 없으면 브로커 inactive 처리 |
| `CONNECT_TIMEOUT_SEC` | 3 | TCP 핸드셰이크 타임아웃 (초) |

### 브로커 브릿지 설정 예시

각 Edge 브로커의 `mosquitto.conf`:

```
connection bridge-to-B3
address 192.168.0.13:1883
topic camera/# out 1 "" b1/b3/

connection bridge-to-B4
address 192.168.0.29:1883
topic camera/# out 1 "" b1/b4/
```

브로커 레이블(`b1/b3/` 등)은 실제 네트워크 구성에 맞게 수정한다.

---

## 실행 순서

**1. 브로커 시작**

4개 브로커 호스트에서 각각 Mosquitto를 적절한 설정으로 실행한다.

**2. Subscriber 시작**

```bash
./subscriber_ws
```

**3. 대시보드 서버 시작**

```bash
python3 web_server.py
# http://<host>:8081 에서 대시보드 접속
```

**4. Publisher 시작**

각 Raspberry Pi에서 실행:

```bash
./publisher
```

---

## 대시보드

`http://<subscriber-host>:8081` 에서 다음을 확인할 수 있다.

- **카메라 그리드:** 모든 카메라의 실시간 영상. 카메라 수에 따라 레이아웃 자동 조정
- **이벤트 로그:** 모션 감지, 카메라 온/오프라인 전환, 경로 변경 이력. localStorage에 저장되어 페이지 새로고침 후에도 유지
- **네트워크 경로 다이어그램:** 현재 활성 브로커와 각 카메라의 데이터 경로를 WebSocket 메시지 기반으로 실시간 갱신
