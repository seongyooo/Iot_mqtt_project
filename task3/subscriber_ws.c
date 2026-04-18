/*
 * subscriber_ws.c
 *
 * Build:
 * gcc -Wall -O2 -o subscriber_ws subscriber_ws.c \
 * -lmosquitto -lws -lpthread
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

/* ── 브로커 failover 목록 (블랙리스트 기능 추가) ────────────────*/
typedef struct {
    const char *host;
    int         port;
    const char *label;
    time_t      dead_until;
} broker_t;

static broker_t BROKERS[] = {
    { "192.168.0.13", 1883, "B3", 0 },
    { "192.168.0.9",  1883, "B4", 0 },
    { "192.168.0.7",  1883, "B1", 0 },
    { "192.168.0.11", 1883, "B2", 0 },
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
static volatile int     g_need_failover = 1; /* 초기 시작 시 failover 타게 함 */
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
    if (rc != 0) { fprintf(stderr, "[MQTT] Connect failed rc=%d\n", rc); return; }
    pthread_mutex_lock(&g_mutex);
    g_connected = 1;
    pthread_mutex_unlock(&g_mutex);
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
               
        /* 백그라운드 연결 실패 시 60초 페널티 부여 */
        BROKERS[g_broker_idx].dead_until = time(NULL) + 60;
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
}

/* ─────────────────────────────────────────────────────────────
 * 무중단 Failover 처리
 * ───────────────────────────────────────────────────────────── */
static void do_failover(void) {
    pthread_mutex_lock(&g_mutex);
    g_connected = 0;
    pthread_mutex_unlock(&g_mutex);

    if (g_mosq) {
        mosquitto_loop_stop(g_mosq, true);
        mosquitto_destroy(g_mosq);
    }

    g_mosq = mosquitto_new("cctv_sub_edge_c", false, NULL);
    if (!g_mosq) { perror("mosquitto_new"); return; }
    
    mosquitto_connect_callback_set(g_mosq, on_mqtt_connect);
    mosquitto_disconnect_callback_set(g_mosq, on_mqtt_disconnect);
    mosquitto_message_callback_set(g_mosq, on_mqtt_message);
    mosquitto_reconnect_delay_set(g_mosq, 1, 5, false);

    time_t now = time(NULL);
    int connected = 0;

    for (int i = 0; i < BROKER_COUNT; i++) {
        g_broker_idx = (g_broker_idx + 1) % BROKER_COUNT;
        
        if (now < BROKERS[g_broker_idx].dead_until) {
            printf("[Failover] Skipping %s (Dead until %ld)\n", 
                   BROKERS[g_broker_idx].label, BROKERS[g_broker_idx].dead_until - now);
            continue; 
        }

        printf("[Failover] Trying %s (%s)...\n", 
               BROKERS[g_broker_idx].host, BROKERS[g_broker_idx].label);

        int rc = mosquitto_connect_async(g_mosq, 
                                         BROKERS[g_broker_idx].host, 
                                         BROKERS[g_broker_idx].port, 5);
                                         
        if (rc == MOSQ_ERR_SUCCESS) {
            connected = 1;
            break; 
        } else {
            printf("[Failover] Failed to connect %s (rc=%d). Blacklisted for 60s.\n", 
                   BROKERS[g_broker_idx].label, rc);
            BROKERS[g_broker_idx].dead_until = now + 60;
        }
    }

    if (connected) {
        g_need_failover = 0;
        mosquitto_loop_start(g_mosq);
    } else {
        printf("[Failover] All brokers are dead or skipped. Sleeping...\n");
        sleep(2);
        g_need_failover = 1;
    }
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

    while (1) {
        if (g_need_failover) do_failover();
        usleep(100 * 1000);
    }

    if (g_mosq) {
        mosquitto_loop_stop(g_mosq, true);
        mosquitto_destroy(g_mosq);
    }
    mosquitto_lib_cleanup();
    return 0;
}