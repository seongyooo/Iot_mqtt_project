/*
 * subscriber_ws.c
 *
 * Build:
 *   gcc -Wall -O2 -o subscriber_ws subscriber_ws.c \
 *       -lmosquitto -lws -lpthread
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
    { "192.168.0.13", 1883, "B3" },
    { "192.168.0.9",  1883, "B4" },
    { "192.168.0.7",  1883, "B1" },
    { "192.168.0.11", 1883, "B2" },
};
#define BROKER_COUNT (int)(sizeof(BROKERS)/sizeof(BROKERS[0]))

/* ── 구독 토픽 ───────────────────────────────────────────────── */
#define TOPIC_FRAME   "camera/+/frame"
#define TOPIC_EVENT   "camera/+/event"
#define TOPIC_STATUS  "camera/+/status"

/* ── 전역 상태 ───────────────────────────────────────────────── */
static pthread_mutex_t  g_mutex         = PTHREAD_MUTEX_INITIALIZER;
static int              g_broker_idx    = 0;
static int              g_connected     = 0;
static volatile int     g_need_failover = 0;
static struct mosquitto *g_mosq         = NULL;

/* MQTT 토픽에서 cam_id 추출: "camera/pi1/frame" → "pi1" */
static void extract_cam_id(const char *topic, char *out, size_t out_len) {
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
 * MQTT 콜백
 * ───────────────────────────────────────────────────────────── */
static void on_mqtt_connect(struct mosquitto *mosq, void *ud, int rc) {
    (void)ud;
    if (rc != 0) { fprintf(stderr, "[MQTT] Connect failed rc=%d\n", rc); return; }
    pthread_mutex_lock(&g_mutex);
    g_connected = 1;
    pthread_mutex_unlock(&g_mutex);
    printf("[MQTT] Connected to %s (%s)\n",
           BROKERS[g_broker_idx].host, BROKERS[g_broker_idx].label);
    /* 재연결 시에도 호출되므로 여기서 구독 등록 */
    mosquitto_subscribe(mosq, NULL, TOPIC_FRAME,  0);
    mosquitto_subscribe(mosq, NULL, TOPIC_EVENT,  2);
    mosquitto_subscribe(mosq, NULL, TOPIC_STATUS, 1);
}

/*
 * ⚠️  콜백은 libmosquitto 내부 네트워크 스레드에서 실행된다.
 *     deadlock 방지를 위해 플래그만 세우고
 *     실제 재연결은 메인 루프에서 처리한다.
 */
static void on_mqtt_disconnect(struct mosquitto *mosq, void *ud, int rc) {
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
                             const struct mosquitto_message *msg) {
    (void)mosq; (void)ud;
    if (!msg->payload || msg->payloadlen <= 0) return;

    /*
     * retain 메시지 무시
     * LWT "offline"이 retain으로 설정되어 구독 시 즉시 전달된다.
     * 처리하면 브라우저 화면이 깜빡이므로 무시한다.
     */
    if (msg->retain) return;

    char cam_id[16];
    extract_cam_id(msg->topic, cam_id, sizeof(cam_id));

    /* ── frame: 순수 JPEG → binary broadcast ──────────────────
     * 새 아키텍처(B1/B2→B3, B1/B2→B4 단방향)에서는
     * 각 프레임이 Sub에 한 경로로만 도달하므로 dedup 불필요.
     */
    if (strstr(msg->topic, "/frame")) {
        ws_sendframe_bcast(WS_PORT,
            (const char *)msg->payload,
            (uint64_t)msg->payloadlen,
            WS_FR_OP_BIN);
        return;
    }

    /* ── event / status: JSON 래퍼 후 text broadcast ──────────
     * payload가 순수 문자열("online", "offline")이면
     * 따옴표를 추가해 유효한 JSON으로 만든다.
     */
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
 * failover — loop_stop → disconnect → connect → loop_start
 * ───────────────────────────────────────────────────────────── */
static void do_failover(struct mosquitto *mosq) {
    g_broker_idx = (g_broker_idx + 1) % BROKER_COUNT;
    printf("[Failover] Switching to %s (%s)\n",
           BROKERS[g_broker_idx].host, BROKERS[g_broker_idx].label);
    mosquitto_loop_stop(mosq, true);
    mosquitto_disconnect(mosq);
    int rc = mosquitto_connect(mosq,
                               BROKERS[g_broker_idx].host,
                               BROKERS[g_broker_idx].port, 60);
    mosquitto_disconnect(mosq);   /* network thread는 유지 — loop_stop 제거 */
    int rc = mosquitto_connect_async(mosq,
                                     BROKERS[g_broker_idx].host,
                                     BROKERS[g_broker_idx].port, 10);
    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "[Failover] Connect failed to %s, will retry\n",
                BROKERS[g_broker_idx].label);
        sleep(1);
        fprintf(stderr, "[Failover] connect_async to %s rc=%d, will retry\n",
                BROKERS[g_broker_idx].label, rc);
        g_need_failover = 1;
        return;
    }
    g_need_failover = 0;          /* loop_start 전에 클리어 — race condition 방지 */
    mosquitto_loop_start(mosq);
    g_need_failover = 0;
    /* loop_start 재호출 안 함 — main에서 1회 시작해 계속 실행 중 */
}

/* ─────────────────────────────────────────────────────────────
@@ -207,31 +205,26 @@
    mosquitto_message_callback_set(g_mosq, on_mqtt_message);
    mosquitto_reconnect_delay_set(g_mosq, 1, 5, false);

    if (mosquitto_connect(g_mosq,
                          BROKERS[g_broker_idx].host,
                          BROKERS[g_broker_idx].port, 60) != MOSQ_ERR_SUCCESS) {
        /*
         * 초기 연결 실패 시 종료하지 않고 failover 루프로 진입.
         * mosquitto_connect가 동기적으로 실패한 경우에만 여기 도달하므로
         * g_need_failover = 1을 세워도 안전하다.
         * (비동기 성공 후 on_connect 전에 체크하는 타이밍 버그 없음)
         */
        fprintf(stderr, "[MQTT] Initial connect failed, will retry...\n");
        g_broker_idx = BROKER_COUNT - 1;
    int rc = mosquitto_connect_async(g_mosq,
                                     BROKERS[g_broker_idx].host,
                                     BROKERS[g_broker_idx].port, 10);
    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "[MQTT] Initial connect_async rc=%d, failover will retry\n", rc);
        g_broker_idx = BROKER_COUNT - 1;   /* do_failover에서 (idx+1)%N으로 idx 0부터 재시도 */
        g_need_failover = 1;
    }
    mosquitto_loop_start(g_mosq);

    /*
     * 수동 메인 루프 — 100ms마다 failover 플래그 확인
     * mosquitto_loop_forever()는 블로킹이라 플래그 체크 불가
     */
    while (1) {
        if (g_need_failover) do_failover(g_mosq);
        usleep(100 * 1000);
    }

    mosquitto_loop_stop(g_mosq, true);
    mosquitto_destroy(g_mosq);
    mosquitto_lib_cleanup();
    return 0;