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

/* ── WebSocket 포트 ── */
#define WS_PORT 8765

/* ── Sub는 Core 브로커(B3, B4)에만 연결 ── */
typedef struct { const char *host; int port; const char *label; } broker_t;
static const broker_t BROKERS[] = {
    { "192.168.0.13", 1883, "B3" },
    { "192.168.0.29", 1883, "B4" },
};
#define BROKER_COUNT (int)(sizeof(BROKERS)/sizeof(BROKERS[0]))
#define CONNECT_TIMEOUT_SEC  3
#define BROKER_TIMEOUT_SEC   5   /* 수신 없으면 inactive */

/* ── 구독 토픽: prefix 포함 전체 ── */
#define TOPIC_ALL "+/+/camera/#"

/* ── 전역 MQTT 상태 ── */
static int              g_broker_idx    = 0;
static int              g_connected     = 0;
static volatile int     g_need_failover = 0;
static time_t           g_connect_start = 0;
static struct mosquitto *g_mosq         = NULL;
static pthread_mutex_t  g_mutex         = PTHREAD_MUTEX_INITIALIZER;

/* ─────────────────────────────────────────
 * 카메라 라우팅 상태 추적
 * ───────────────────────────────────────── */
#define MAX_CAMS 10
typedef struct {
    char cam_id[16];
    char last_from[8];   /* 마지막 edge broker: b1 or b2 */
    char last_via[8];    /* 마지막 core broker: b3 or b4 */
} CamStatus;

static CamStatus       g_cam_status[MAX_CAMS];
static int             g_cam_count = 0;
static pthread_mutex_t g_cam_mutex = PTHREAD_MUTEX_INITIALIZER;

/* ─────────────────────────────────────────
 * 브로커별 마지막 수신 시각
 * b1=0, b2=1, b3=2, b4=3
 * ───────────────────────────────────────── */
static time_t          g_broker_seen[4] = {0};
static pthread_mutex_t g_broker_mutex   = PTHREAD_MUTEX_INITIALIZER;

static int broker_to_idx(const char *name)
{
    if (strcasecmp(name, "b1") == 0) return 0;
    if (strcasecmp(name, "b2") == 0) return 1;
    if (strcasecmp(name, "b3") == 0) return 2;
    if (strcasecmp(name, "b4") == 0) return 3;
    return -1;
}

/* ─────────────────────────────────────────
 * 토픽 파싱: b1/b3/camera/pi1/frame
 *   → from=b1, via=b3, cam=pi1, type=frame
 * ───────────────────────────────────────── */
static int parse_topic(const char *topic,
                        char *from, char *via,
                        char *cam,  char *type)
{
    return (sscanf(topic,
                   "%7[^/]/%7[^/]/camera/%15[^/]/%15s",
                   from, via, cam, type) == 4) ? 0 : -1;
}

/* ─────────────────────────────────────────
 * 라우팅 경로 변경 감지 → WebSocket 알림
 * ───────────────────────────────────────── */
static void update_route(const char *cam_id,
                          const char *from, const char *via)
{
    pthread_mutex_lock(&g_cam_mutex);

    int ci = -1;
    for (int i = 0; i < g_cam_count; i++)
        if (strcmp(g_cam_status[i].cam_id, cam_id) == 0) { ci = i; break; }

    if (ci < 0 && g_cam_count < MAX_CAMS) {
        ci = g_cam_count++;
        strncpy(g_cam_status[ci].cam_id, cam_id, 15);
        g_cam_status[ci].last_from[0] = '\0';
        g_cam_status[ci].last_via[0]  = '\0';
    }
    if (ci < 0) { pthread_mutex_unlock(&g_cam_mutex); return; }

    int changed = (strcmp(g_cam_status[ci].last_from, from) != 0 ||
                   strcmp(g_cam_status[ci].last_via,  via)  != 0);
    if (changed) {
        printf("[Route Changed] cam=%s: %s→%s → %s→%s\n",
               cam_id,
               g_cam_status[ci].last_from[0] ? g_cam_status[ci].last_from : "?",
               g_cam_status[ci].last_via[0]  ? g_cam_status[ci].last_via  : "?",
               from, via);

        strncpy(g_cam_status[ci].last_from, from, 7);
        strncpy(g_cam_status[ci].last_via,  via,  7);

        /* 브라우저에 경로 변경 알림 */
        char json[256];
        snprintf(json, sizeof(json),
                 "{\"type\":\"route\",\"cam\":\"%s\","
                 "\"from\":\"%s\",\"via\":\"%s\"}",
                 cam_id, from, via);
        ws_sendframe_bcast(WS_PORT, json, strlen(json), WS_FR_OP_TXT);
    }
    pthread_mutex_unlock(&g_cam_mutex);
}

/* ─────────────────────────────────────────
 * 브로커 Watchdog 스레드
 * 수신 타임아웃 → inactive 알림
 * ───────────────────────────────────────── */
static void *broker_watchdog(void *arg)
{
    (void)arg;
    const char *names[]  = {"b1","b2","b3","b4"};
    int prev_active[4]   = {-1,-1,-1,-1};

    while (1) {
        sleep(2);
        time_t now = time(NULL);
        pthread_mutex_lock(&g_broker_mutex);
        for (int i = 0; i < 4; i++) {
            if (g_broker_seen[i] == 0) continue;
            int active = (now - g_broker_seen[i]) < BROKER_TIMEOUT_SEC;
            if (active != prev_active[i]) {
                prev_active[i] = active;
                char json[128];
                snprintf(json, sizeof(json),
                         "{\"type\":\"broker_status\","
                         "\"broker\":\"%s\",\"active\":%s}",
                         names[i], active ? "true" : "false");
                ws_sendframe_bcast(WS_PORT, json, strlen(json), WS_FR_OP_TXT);
                printf("[Broker] %s → %s\n",
                       names[i], active ? "active" : "inactive");
            }
        }
        pthread_mutex_unlock(&g_broker_mutex);
    }
    return NULL;
}

/* ─────────────────────────────────────────
 * WebSocket 콜백
 * ───────────────────────────────────────── */
void onopen(ws_cli_conn_t client)
{
    printf("[WS] Connected: %s\n", ws_getaddress(client));

    /* 신규 클라이언트에게 현재 라우팅 상태 즉시 전송 */
    pthread_mutex_lock(&g_cam_mutex);
    for (int i = 0; i < g_cam_count; i++) {
        if (!g_cam_status[i].last_from[0]) continue;
        char json[256];
        snprintf(json, sizeof(json),
                 "{\"type\":\"route\",\"cam\":\"%s\","
                 "\"from\":\"%s\",\"via\":\"%s\"}",
                 g_cam_status[i].cam_id,
                 g_cam_status[i].last_from,
                 g_cam_status[i].last_via);
        ws_sendframe_bcast(WS_PORT, json, strlen(json), WS_FR_OP_TXT);
    }
    pthread_mutex_unlock(&g_cam_mutex);
}

void onclose(ws_cli_conn_t client) {
    printf("[WS] Disconnected: %s\n", ws_getaddress(client));
}

void onmessage(ws_cli_conn_t client,
               const unsigned char *msg, uint64_t size, int type) {
    (void)client; (void)msg; (void)size; (void)type;
}

/* ─────────────────────────────────────────
 * MQTT 콜백
 * ───────────────────────────────────────── */
static void on_mqtt_connect(struct mosquitto *mosq, void *ud, int rc)
{
    (void)ud;
    if (rc != 0) {
        fprintf(stderr, "[MQTT] Connect failed rc=%d\n", rc);
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

    mosquitto_subscribe(mosq, NULL, TOPIC_ALL, 1);

    /* 브라우저에 Sub 연결 브로커 알림 */
    char json[128];
    snprintf(json, sizeof(json),
             "{\"type\":\"sub_connected\",\"broker\":\"%s\"}",
             BROKERS[g_broker_idx].label);
    ws_sendframe_bcast(WS_PORT, json, strlen(json), WS_FR_OP_TXT);
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

    /* ── 토픽 파싱 ── */
    char from[8], via[8], cam_id[16], sub_type[16];
    if (parse_topic(msg->topic, from, via, cam_id, sub_type) != 0) return;

    /* ── 브로커 수신 시각 갱신 ── */
    pthread_mutex_lock(&g_broker_mutex);
    int fi = broker_to_idx(from);
    int vi = broker_to_idx(via);
    if (fi >= 0) g_broker_seen[fi] = time(NULL);
    if (vi >= 0) g_broker_seen[vi] = time(NULL);
    pthread_mutex_unlock(&g_broker_mutex);

    /* ── 라우팅 경로 변경 감지 ── */
    update_route(cam_id, from, via);

    /* ── 메시지 타입별 처리 ── */
    if (strcmp(sub_type, "frame") == 0) {
        /* 프레임: [1B: id_len][cam_id][JPEG] */
        uint8_t  id_len = (uint8_t)strlen(cam_id);
        size_t   total  = 1 + id_len + (size_t)msg->payloadlen;
        uint8_t *buf    = malloc(total);
        if (!buf) return;
        buf[0] = id_len;
        memcpy(buf + 1,          cam_id,       id_len);
        memcpy(buf + 1 + id_len, msg->payload, msg->payloadlen);
        ws_sendframe_bcast(WS_PORT, (const char *)buf,
                           (uint64_t)total, WS_FR_OP_BIN);
        free(buf);

    } else {
        /* 이벤트 / 상태: JSON 래퍼 + from/via 포함 */
        char wrapper[1024];
        const char *type        = (strcmp(sub_type, "event") == 0) ? "event" : "status";
        const char *payload_str = (const char *)msg->payload;

        if (payload_str[0] == '{' || payload_str[0] == '[') {
            snprintf(wrapper, sizeof(wrapper),
                     "{\"type\":\"%s\",\"cam\":\"%s\","
                     "\"from\":\"%s\",\"via\":\"%s\","
                     "\"data\":%.*s}",
                     type, cam_id, from, via,
                     msg->payloadlen, payload_str);
        } else {
            snprintf(wrapper, sizeof(wrapper),
                     "{\"type\":\"%s\",\"cam\":\"%s\","
                     "\"from\":\"%s\",\"via\":\"%s\","
                     "\"data\":\"%.*s\"}",
                     type, cam_id, from, via,
                     msg->payloadlen, payload_str);
        }
        ws_sendframe_bcast(WS_PORT, wrapper,
                           (uint64_t)strlen(wrapper), WS_FR_OP_TXT);
        printf("[MQTT] %s/%s via %s→%s\n", type, cam_id, from, via);
    }
}

/* ─────────────────────────────────────────
 * 콜백 설정 / mosq_stop / failover / init
 * ───────────────────────────────────────── */
static void setup_callbacks(struct mosquitto *mosq) {
    mosquitto_connect_callback_set(mosq, on_mqtt_connect);
    mosquitto_disconnect_callback_set(mosq, on_mqtt_disconnect);
    mosquitto_message_callback_set(mosq, on_mqtt_message);
}

static void mosq_stop(struct mosquitto *mosq) {
    mosquitto_disconnect(mosq);
    mosquitto_loop_stop(mosq, true);
}

static void do_failover(void) {
    g_broker_idx = (g_broker_idx + 1) % BROKER_COUNT;
    printf("[Failover] Switching to %s (%s)\n",
           BROKERS[g_broker_idx].host, BROKERS[g_broker_idx].label);
    mosq_stop(g_mosq);
    mosquitto_destroy(g_mosq);
    g_mosq = mosquitto_new("cctv_sub_edge_c", true, NULL);
    if (!g_mosq) { perror("[Failover] mosquitto_new"); g_need_failover = 1; return; }
    setup_callbacks(g_mosq);
    g_need_failover = 0;
    g_connect_start = time(NULL);
    int rc = mosquitto_connect_async(g_mosq,
                                     BROKERS[g_broker_idx].host,
                                     BROKERS[g_broker_idx].port, 10);
    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "[Failover] connect_async failed rc=%d\n", rc);
        g_connect_start = 0; g_need_failover = 1; return;
    }
    mosquitto_loop_start(g_mosq);
}

static struct mosquitto *mqtt_init(void) {
    mosquitto_lib_init();
    struct mosquitto *mosq = mosquitto_new("cctv_sub_edge_c", true, NULL);
    if (!mosq) { perror("mosquitto_new"); return NULL; }
    setup_callbacks(mosq);
    for (int i = 0; i < BROKER_COUNT; i++) {
        int rc = mosquitto_connect_async(mosq,
                                         BROKERS[i].host, BROKERS[i].port, 10);
        if (rc == MOSQ_ERR_SUCCESS) {
            g_broker_idx = i; g_connect_start = time(NULL);
            printf("[MQTT] Initial connect_async to %s (%s)\n",
                   BROKERS[i].host, BROKERS[i].label);
            break;
        }
    }
    mosquitto_loop_start(mosq);
    return mosq;
}

/* ─────────────────────────────────────────
 * main
 * ───────────────────────────────────────── */
int main(void) {
    ws_socket(&(struct ws_server){
        .host = "0.0.0.0", .port = WS_PORT,
        .thread_loop = 1, .timeout_ms = 1000,
        .evs.onopen = &onopen, .evs.onclose = &onclose,
        .evs.onmessage = &onmessage,
    });
    printf("[WS] Server started on port %d\n", WS_PORT);

    pthread_t watchdog_tid;
    pthread_create(&watchdog_tid, NULL, broker_watchdog, NULL);
    pthread_detach(watchdog_tid);

    g_mosq = mqtt_init();
    if (!g_mosq) return -1;

    while (1) {
        if (g_need_failover) { do_failover(); continue; }
        if (!g_connected && g_connect_start > 0 &&
            time(NULL) - g_connect_start > CONNECT_TIMEOUT_SEC) {
            g_connect_start = 0; g_need_failover = 1;
        }
        usleep(100000);
    }
    mosq_stop(g_mosq);
    mosquitto_destroy(g_mosq);
    mosquitto_lib_cleanup();
    return 0;
}