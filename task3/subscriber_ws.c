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
#include <sys/socket.h>
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
static time_t           g_connect_start = 0;
#define CONNECT_TIMEOUT_SEC 3
static struct mosquitto *g_mosq         = NULL;

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
    if (rc != 0) {
        fprintf(stderr, "[MQTT] Connect failed rc=%d\n", rc);
        return;
    }
    /* TCP timeout이 g_need_failover=1을 세운 직후 on_connect가
     * 도착할 수 있으므로 여기서 클리어한다. */
    g_need_failover = 0;
    pthread_mutex_lock(&g_mutex);
    g_connected = 1;
    pthread_mutex_unlock(&g_mutex);
    g_connect_start = 0;
    printf("[MQTT] Connected to %s (%s)\n",
           BROKERS[g_broker_idx].host, BROKERS[g_broker_idx].label);
    mosquitto_subscribe(mosq, NULL, TOPIC_FRAME,  0);
    mosquitto_subscribe(mosq, NULL, TOPIC_EVENT,  2);
    mosquitto_subscribe(mosq, NULL, TOPIC_STATUS, 1);
}

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
    if (msg->retain) return;

    char cam_id[16];
    extract_cam_id(msg->topic, cam_id, sizeof(cam_id));

    if (strstr(msg->topic, "/frame")) {
        ws_sendframe_bcast(WS_PORT,
            (const char *)msg->payload,
            (uint64_t)msg->payloadlen,
            WS_FR_OP_BIN);
        return;
    }

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
 * 콜백 설정 — 인스턴스 생성 후 공통 초기화
 * ───────────────────────────────────────────────────────────── */
static void setup_callbacks(struct mosquitto *mosq) {
    mosquitto_connect_callback_set(mosq, on_mqtt_connect);
    mosquitto_disconnect_callback_set(mosq, on_mqtt_disconnect);
    mosquitto_message_callback_set(mosq, on_mqtt_message);
}

/* ─────────────────────────────────────────────────────────────
 * mosq_stop — 소켓 강제 종료 후 loop_stop
 *
 * mosquitto_disconnect()는 connect_pending 상태(TCP SYN 전송 후 대기)일 때
 * 소켓을 닫지 않고 MOSQ_ERR_NO_CONN을 반환한다.
 * 이 경우 loop_stop이 select()에서 무기한 블로킹된다.
 *
 * shutdown(SHUT_RDWR)으로 소켓을 먼저 강제 종료하면:
 *   → select()가 즉시 오류 반환
 *   → mosquitto_loop()가 에러 코드 반환
 *   → 네트워크 스레드 자연 종료
 *   → loop_stop(false)가 빠르게 리턴 (pthread_cancel 없음 → mutex 안전)
 * ───────────────────────────────────────────────────────────── */
static void mosq_stop(struct mosquitto *mosq) {
    int sock = mosquitto_socket(mosq);
    if (sock >= 0) shutdown(sock, SHUT_RDWR);
    mosquitto_disconnect(mosq);
    mosquitto_loop_stop(mosq, false);
}

/* ─────────────────────────────────────────────────────────────
 * failover — 인스턴스 destroy → 재생성 → connect_async
 * ───────────────────────────────────────────────────────────── */
static void do_failover(void) {
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
        fprintf(stderr, "[Failover] connect_async to %s rc=%d, will retry\n",
                BROKERS[g_broker_idx].label, rc);
        g_connect_start = 0;
        g_need_failover = 1;
        return;
    }
    mosquitto_loop_start(g_mosq);
}

/* ─────────────────────────────────────────────────────────────
 * main
 * ───────────────────────────────────────────────────────────── */
int main(void) {
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

    mosquitto_lib_init();
    g_mosq = mosquitto_new("cctv_sub_edge_c", true, NULL);
    if (!g_mosq) { perror("mosquitto_new"); return -1; }
    setup_callbacks(g_mosq);

    int rc = mosquitto_connect_async(g_mosq,
                                     BROKERS[g_broker_idx].host,
                                     BROKERS[g_broker_idx].port, 10);
    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "[MQTT] Initial connect_async rc=%d\n", rc);
        g_broker_idx = BROKER_COUNT - 1;
        g_need_failover = 1;
    } else {
        printf("[MQTT] Initial connect_async to %s (%s)\n",
               BROKERS[0].host, BROKERS[0].label);
        g_connect_start = time(NULL);
    }
    mosquitto_loop_start(g_mosq);

    while (1) {
        if (g_need_failover) do_failover();

        if (!g_connected && g_connect_start > 0 &&
            time(NULL) - g_connect_start > CONNECT_TIMEOUT_SEC) {
            printf("[Failover] TCP timeout on %s, switching next broker\n",
                   BROKERS[g_broker_idx].label);
            g_connect_start = 0;
            g_need_failover = 1;
        }

        usleep(100 * 1000);
    }

    mosq_stop(g_mosq);
    mosquitto_destroy(g_mosq);
    mosquitto_lib_cleanup();
    return 0;
}