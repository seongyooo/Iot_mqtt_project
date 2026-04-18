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
 * MQTT 콜백 — 모두 메인 스레드(mosquitto_loop) 컨텍스트에서 호출됨
 * ───────────────────────────────────────────────────────────── */
static void on_mqtt_connect(struct mosquitto *mosq, void *ud, int rc) {
    (void)ud;
    if (rc != 0) {
        fprintf(stderr, "[MQTT] Connect failed rc=%d (%s)\n",
                rc, mosquitto_strerror(rc));
        return;
    }
    g_need_failover = 0;
    g_connected     = 1;
    g_connect_start = 0;
    printf("[MQTT] Connected to %s (%s)\n",
           BROKERS[g_broker_idx].host, BROKERS[g_broker_idx].label);
    /* 비스레드 모드: mosquitto_loop() 안에서 호출되므로
     * mosquitto_subscribe 호출이 안전하다. */
    mosquitto_subscribe(mosq, NULL, TOPIC_FRAME,  0);
    mosquitto_subscribe(mosq, NULL, TOPIC_EVENT,  2);
    mosquitto_subscribe(mosq, NULL, TOPIC_STATUS, 1);
}

static void on_mqtt_disconnect(struct mosquitto *mosq, void *ud, int rc) {
    (void)ud; (void)mosq;
    g_connected = 0;
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
 * failover — 인스턴스 destroy → 재생성 → connect_async
 *
 * loop_start/loop_stop을 사용하지 않는다.
 * mosquitto 네트워크 루프는 메인 루프에서 mosquitto_loop()로 직접
 * 구동하므로, pthread_cancel에 의한 글로벌 상태 오염이 없다.
 *
 * connect_pending(TCP SYN 대기) 상태에서도 단순히 destroy하면 되고
 * 스레드 종료를 기다릴 필요가 없다.
 * ───────────────────────────────────────────────────────────── */
static void do_failover(void) {
    g_broker_idx = (g_broker_idx + 1) % BROKER_COUNT;
    printf("[Failover] Switching to %s (%s)\n",
           BROKERS[g_broker_idx].host, BROKERS[g_broker_idx].label);

    mosquitto_disconnect(g_mosq);
    mosquitto_destroy(g_mosq);   /* 스레드 없음 → 단순 소멸 */

    g_mosq = mosquitto_new("cctv_sub_edge_c", true, NULL);
    if (!g_mosq) {
        perror("[Failover] mosquitto_new");
        g_need_failover = 1;
        return;
    }
    setup_callbacks(g_mosq);

    g_need_failover = 0;
    g_connected     = 0;
    g_connect_start = time(NULL);

    int rc = mosquitto_connect_async(g_mosq,
                                     BROKERS[g_broker_idx].host,
                                     BROKERS[g_broker_idx].port, 10);
    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "[Failover] connect_async to %s rc=%d (%s)\n",
                BROKERS[g_broker_idx].label, rc, mosquitto_strerror(rc));
        g_connect_start = 0;
        g_need_failover = 1;
    }
    /* loop_start 호출 없음 — 메인 루프의 mosquitto_loop()가 구동한다 */
}

/* ─────────────────────────────────────────────────────────────
 * main
 * ───────────────────────────────────────────────────────────── */
int main(void) {
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
    /* loop_start 없음 — 아래 메인 루프에서 mosquitto_loop()로 직접 구동 */

    while (1) {
        /* failover 처리 */
        if (g_need_failover) {
            do_failover();
            continue; /* 새 인스턴스로 즉시 loop 재진입 */
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
         * mosquitto 네트워크 루프를 메인 스레드에서 직접 구동.
         * timeout_ms=100: 100ms마다 failover 플래그를 확인할 수 있다.
         * loop_start/pthread_cancel 없음 → 글로벌 상태 오염 위험 없음.
         */
        rc = mosquitto_loop(g_mosq, 100, 1);
        if (rc != MOSQ_ERR_SUCCESS && rc != MOSQ_ERR_NO_CONN) {
            if (!g_need_failover) {
                printf("[MQTT] Loop error rc=%d (%s), triggering failover\n",
                       rc, mosquitto_strerror(rc));
                g_need_failover = 1;
            }
        }
    }

    mosquitto_disconnect(g_mosq);
    mosquitto_destroy(g_mosq);
    mosquitto_lib_cleanup();
    return 0;
}