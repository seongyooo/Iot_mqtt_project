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
        fprintf(stderr, "[MQTT] Connect failed rc=%d (%s)\n",
                rc, mosquitto_strerror(rc));
        return;
    }
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
 * 콜백 설정
 * ───────────────────────────────────────────────────────────── */
static void setup_callbacks(struct mosquitto *mosq) {
    mosquitto_connect_callback_set(mosq, on_mqtt_connect);
    mosquitto_disconnect_callback_set(mosq, on_mqtt_disconnect);
    mosquitto_message_callback_set(mosq, on_mqtt_message);
}

/* ─────────────────────────────────────────────────────────────
 * mosq_stop — 네트워크 스레드 강제 종료
 *
 * mosquitto_disconnect()는 connect_pending 상태(TCP SYN 대기)일 때
 * 소켓을 닫지 않고 MOSQ_ERR_NO_CONN을 반환한다.
 * loop_stop(false)만 쓰면 select()가 블로킹되어 수십 초 대기한다.
 *
 * loop_stop(true): pthread_cancel로 스레드 강제 종료 후 pthread_join.
 * 취소된 스레드가 보유한 mutex는 per-instance이므로,
 * 바로 뒤에 mosquitto_destroy로 인스턴스를 소멸시키면 안전하다.
 *
 * shutdown(sock)을 쓰지 않는 이유:
 * shutdown이 소켓을 강제 닫으면 mosquitto 내부에서 close(fd)를 중복
 * 호출하거나, 새 소켓이 같은 fd 번호를 재사용해 connect_async에서
 * MOSQ_ERR_ERRNO(rc=14) 오류가 발생할 수 있다.
 * ───────────────────────────────────────────────────────────── */
static void mosq_stop(struct mosquitto *mosq) {
    mosquitto_disconnect(mosq);      /* MQTT DISCONNECT 시도 (connect_pending이면 no-op) */
    mosquitto_loop_stop(mosq, true); /* pthread_cancel + pthread_join — 즉시 종료 보장 */
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
        fprintf(stderr, "[Failover] connect_async to %s rc=%d (%s)\n",
                BROKERS[g_broker_idx].label, rc, mosquitto_strerror(rc));
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
        fprintf(stderr, "[MQTT] Initial connect_async rc=%d (%s)\n",
                rc, mosquitto_strerror(rc));
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