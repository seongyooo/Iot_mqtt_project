/*
 * subscriber_ws.c  (수정판)
 *
 * Build:
 *   gcc -Wall -O2 -o subscriber_ws subscriber_ws.c \
 *       -lmosquitto -lws -lpthread
 *
 * ── 핵심 변경점 ──────────────────────────────────────────────
 *  1. mosquitto_loop() 단일-스레드 루프 제거
 *     → loop_start / loop_stop(true) 방식으로 publisher와 동일하게 통일
 *
 *  2. mosq_stop() 도입
 *     connect_pending(TCP SYN 대기) 상태에서도 pthread_cancel로
 *     백그라운드 스레드를 즉시 종료하여 소켓 누수 방지
 *
 *  3. g_connected mutex 보호 추가
 *     loop_start 백그라운드 스레드에서 콜백이 호출되므로
 *     publisher와 동일하게 pthread_mutex 로 보호
 *
 *  4. mqtt_init() 분리
 *     publisher 패턴을 그대로 이식; 초기 브로커 순회 동일
 *
 *  5. 메인 루프 단순화
 *     mosquitto_loop() 호출 제거 → usleep(100ms) 폴링으로 대체
 * ────────────────────────────────────────────────────────────
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <mosquitto.h>
#include <wsserver/ws.h>

/* ── WebSocket 포트 ──────────────────────────────────────────── */
#define WS_PORT 8765

/* ── 브로커 failover 목록 ────────────────────────────────────── */
typedef struct {
    const char *host;
    int         port;
    const char *label;
} broker_t;

static const broker_t BROKERS[] = {
    { "192.168.0.29", 1883, "B3" },
    { "192.168.0.8",  1883, "B4" },
    { "192.168.0.7",  1883, "B1" },
    { "192.168.0.11", 1883, "B2" },
};
#define BROKER_COUNT (int)(sizeof(BROKERS)/sizeof(BROKERS[0]))

/* ── 구독 토픽 ───────────────────────────────────────────────── */
#define TOPIC_FRAME   "camera/+/frame"
#define TOPIC_EVENT   "camera/+/event"
#define TOPIC_STATUS  "camera/+/status"

/* ── 전역 상태 ───────────────────────────────────────────────── */
#define CONNECT_TIMEOUT_SEC 3

static int              g_broker_idx    = 0;
static int              g_connected     = 0;
static volatile int     g_need_failover = 0;
static time_t           g_connect_start = 0;
static struct mosquitto *g_mosq         = NULL;
static pthread_mutex_t  g_mutex         = PTHREAD_MUTEX_INITIALIZER;

/* ─────────────────────────────────────────────────────────────
 * 유틸 — cam ID 추출
 * ───────────────────────────────────────────────────────────── */
static void extract_cam_id(const char *topic, char *out, size_t out_len)
{
    const char *p = strchr(topic, '/');
    if (!p) { strncpy(out, "unknown", out_len); return; }
    p++;
    const char *q = strchr(p, '/');
    if (!q) { strncpy(out, "unknown", out_len); return; }
    size_t len = (size_t)(q - p);
    if (len >= out_len) len = out_len - 1;
    strncpy(out, p, len);
    out[len] = '\0';
}

/* ─────────────────────────────────────────────────────────────
 * WebSocket 콜백
 * ───────────────────────────────────────────────────────────── */
void onopen(ws_cli_conn_t client) {
    printf("[WS] Connected: %s\n", ws_getaddress(client));
}
void onclose(ws_cli_conn_t client) {
    printf("[WS] Disconnected: %s\n", ws_getaddress(client));
}
void onmessage(ws_cli_conn_t client,
               const unsigned char *msg, uint64_t size, int type) {
    (void)client; (void)msg; (void)size; (void)type;
}

/* ─────────────────────────────────────────────────────────────
 * MQTT 콜백 — loop_start 사용 시 백그라운드 스레드에서 호출됨
 * ───────────────────────────────────────────────────────────── */
static void on_mqtt_connect(struct mosquitto *mosq, void *ud, int rc)
{
    (void)ud;
    if (rc != 0) {
        fprintf(stderr, "[MQTT] Connect failed rc=%d (%s)\n",
                rc, mosquitto_strerror(rc));
        g_need_failover = 1;
        return;
    }
    g_need_failover = 0;
    pthread_mutex_lock(&g_mutex);
    g_connected = 1;
    pthread_mutex_unlock(&g_mutex);
    g_connect_start = 0;

    printf("[MQTT] Connected to %s (%s)\n",
           BROKERS[g_broker_idx].host, BROKERS[g_broker_idx].label);

    /* loop_start 스레드 컨텍스트에서 mosquitto_subscribe 호출은 안전 */
    mosquitto_subscribe(mosq, NULL, TOPIC_FRAME,  0);
    mosquitto_subscribe(mosq, NULL, TOPIC_EVENT,  2);
    mosquitto_subscribe(mosq, NULL, TOPIC_STATUS, 1);
}

static void on_mqtt_disconnect(struct mosquitto *mosq, void *ud, int rc)
{
    (void)ud; (void)mosq;
    pthread_mutex_lock(&g_mutex);
    g_connected = 0;
    pthread_mutex_unlock(&g_mutex);
    if (rc != 0) {
        printf("[MQTT] Disconnected from %s. Triggering failover...\n",
               BROKERS[g_broker_idx].label);
        g_need_failover = 1;
    }
}

static void on_mqtt_message(struct mosquitto *mosq, void *ud,
                             const struct mosquitto_message *msg)
{
    (void)mosq; (void)ud;
    if (!msg->payload || msg->payloadlen <= 0) return;
    if (msg->retain) return;

    char cam_id[16];
    extract_cam_id(msg->topic, cam_id, sizeof(cam_id));

    /* 프레임: 바이너리 브로드캐스트 */
    if (strstr(msg->topic, "/frame")) {
        ws_sendframe_bcast(WS_PORT,
            (const char *)msg->payload,
            (uint64_t)msg->payloadlen,
            WS_FR_OP_BIN);
        return;
    }

    /* 이벤트 / 상태: JSON 래퍼 후 텍스트 브로드캐스트 */
    char wrapper[1024];
    const char *type        = strstr(msg->topic, "/event") ? "event" : "status";
    const char *payload_str = (const char *)msg->payload;

    if (payload_str[0] == '{' || payload_str[0] == '[') {
        snprintf(wrapper, sizeof(wrapper),
                 "{\"type\":\"%s\",\"cam\":\"%s\",\"data\":%.*s}",
                 type, cam_id, msg->payloadlen, payload_str);
    } else {
        snprintf(wrapper, sizeof(wrapper),
                 "{\"type\":\"%s\",\"cam\":\"%s\",\"data\":\"%.*s\"}",
                 type, cam_id, msg->payloadlen, payload_str);
    }

    ws_sendframe_bcast(WS_PORT,
        wrapper, (uint64_t)strlen(wrapper), WS_FR_OP_TXT);
    printf("[MQTT] %s/%s → WS: %s\n", type, cam_id, wrapper);
}

/* ─────────────────────────────────────────────────────────────
 * 콜백 설정
 * ───────────────────────────────────────────────────────────── */
static void setup_callbacks(struct mosquitto *mosq)
{
    mosquitto_connect_callback_set(mosq, on_mqtt_connect);
    mosquitto_disconnect_callback_set(mosq, on_mqtt_disconnect);
    mosquitto_message_callback_set(mosq, on_mqtt_message);
}

/* ─────────────────────────────────────────────────────────────
 * mosq_stop — publisher와 동일한 패턴
 *
 * mosquitto_disconnect()는 connect_pending(TCP SYN 대기) 상태에서
 * 소켓을 닫지 않고 MOSQ_ERR_NO_CONN을 반환한다.
 * loop_stop(true): pthread_cancel + pthread_join으로 백그라운드
 * 스레드를 즉시 강제 종료 → connect_pending 소켓도 함께 정리됨.
 * ───────────────────────────────────────────────────────────── */
static void mosq_stop(struct mosquitto *mosq)
{
    mosquitto_disconnect(mosq);       /* MQTT DISCONNECT 시도 */
    mosquitto_loop_stop(mosq, true);  /* pthread_cancel + join → 즉시 종료 보장 */
}

/* ─────────────────────────────────────────────────────────────
 * failover — publisher와 동일한 패턴
 *   mosq_stop → destroy → new → setup_callbacks
 *   → connect_async → loop_start
 * ───────────────────────────────────────────────────────────── */
static void do_failover(void)
{
    g_broker_idx = (g_broker_idx + 1) % BROKER_COUNT;
    printf("[Failover] Switching to %s (%s)\n",
           BROKERS[g_broker_idx].host, BROKERS[g_broker_idx].label);

    mosq_stop(g_mosq);
    mosquitto_destroy(g_mosq);

    g_mosq = mosquitto_new("cctv_sub_edge_c", true, NULL);
    if (!g_mosq) {
        perror("[Failover] mosquitto_new");
        g_need_failover = 1;
        return;
    }
    setup_callbacks(g_mosq);

    g_need_failover = 0;
    g_connect_start = time(NULL);

    int rc = mosquitto_connect_async(g_mosq,
                                     BROKERS[g_broker_idx].host,
                                     BROKERS[g_broker_idx].port, 10);
    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "[Failover] connect_async to %s rc=%d (%s)\n",
                BROKERS[g_broker_idx].label, rc, mosquitto_strerror(rc));
        g_connect_start = 0;
        g_need_failover = 1;
        return;
    }
    mosquitto_loop_start(g_mosq); /* 백그라운드 스레드 시작 */
}

/* ─────────────────────────────────────────────────────────────
 * MQTT 초기화 — publisher 패턴 이식
 * ───────────────────────────────────────────────────────────── */
static struct mosquitto *mqtt_init(void)
{
    mosquitto_lib_init();
    struct mosquitto *mosq = mosquitto_new("cctv_sub_edge_c", true, NULL);
    if (!mosq) { perror("mosquitto_new"); return NULL; }
    setup_callbacks(mosq);

    for (int i = 0; i < BROKER_COUNT; i++) {
        int rc = mosquitto_connect_async(mosq, BROKERS[i].host, BROKERS[i].port, 10);
        if (rc == MOSQ_ERR_SUCCESS) {
            g_broker_idx    = i;
            g_connect_start = time(NULL);
            printf("[MQTT] Initial connect_async to %s (%s)\n",
                   BROKERS[i].host, BROKERS[i].label);
            break;
        }
        fprintf(stderr, "[MQTT] %s (%s) connect_async failed rc=%d (%s)\n",
                BROKERS[i].host, BROKERS[i].label, rc, mosquitto_strerror(rc));
    }
    mosquitto_loop_start(mosq); /* 백그라운드 네트워크 스레드 시작 */
    return mosq;
}

/* ─────────────────────────────────────────────────────────────
 * main
 * ───────────────────────────────────────────────────────────── */
int main(void)
{
    /* WebSocket 서버 — 별도 스레드 */
    ws_socket(&(struct ws_server){
        .host          = "0.0.0.0",
        .port          = WS_PORT,
        .thread_loop   = 1,
        .timeout_ms    = 1000,
        .evs.onopen    = &onopen,
        .evs.onclose   = &onclose,
        .evs.onmessage = &onmessage,
    });
    printf("[WS] Server started on port %d\n", WS_PORT);

    g_mosq = mqtt_init();
    if (!g_mosq) return -1;

    while (1) {
        /* failover 처리 */
        if (g_need_failover) {
            do_failover();
            continue;
        }

        /* TCP 연결 타임아웃 감지 */
        if (!g_connected && g_connect_start > 0 &&
            time(NULL) - g_connect_start > CONNECT_TIMEOUT_SEC) {
            printf("[Failover] TCP timeout on %s, switching next broker\n",
                   BROKERS[g_broker_idx].label);
            g_connect_start = 0;
            g_need_failover = 1;
            continue;
        }

        /*
         * mosquitto 루프는 loop_start 백그라운드 스레드가 담당.
         * 메인 스레드는 100ms 간격으로 failover 상태만 감시.
         * mosquitto_loop() 호출 불필요.
         */
        usleep(100000);
    }

    /* 도달하지 않지만 정리 코드 유지 */
    mosq_stop(g_mosq);
    mosquitto_destroy(g_mosq);
    mosquitto_lib_cleanup();
    return 0;
}