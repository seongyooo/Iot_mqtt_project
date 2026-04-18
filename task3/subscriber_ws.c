/*
 * subscriber_ws.c
 *
 * 수정 내역:
 *   1. on_disconnect → 플래그 방식 (deadlock 방지)
 *   2. failover 재연결 → loop_stop → disconnect → connect 방식 (민준이 방식)
 *   3. dedup → 해시 테이블 기반 O(1) 중복 제거
 *   4. mosquitto_loop_forever → 수동 루프로 교체 (플래그 체크 가능)
 *   5. retain 메시지 무시 (offline 깜빡임 방지)
 *   6. payload JSON 따옴표 처리 (online/offline 문자열)
 *   7. onopen 캐시 전송 제거 (실시간성 보장)
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

/* ─────────────────────────────────────────────────────────────
 * dedup 자료구조 — 해시 테이블 기반 O(1)
 * ───────────────────────────────────────────────────────────── */
#define DEDUP_SIZE  8192
#define TTL_SECONDS 10
#define ID_MAX_LEN  64

typedef struct {
    char   id[ID_MAX_LEN];
    time_t timestamp;
    int    occupied;
} SeenMsg;

static SeenMsg seen[DEDUP_SIZE];

static unsigned int hash_id(const char *id) {
    unsigned int h = 0;
    while (*id) h = h * 31 + (unsigned char)*id++;
    return h % DEDUP_SIZE;
}

static void cleanup_expired(void) {
    time_t now = time(NULL);
    for (int i = 0; i < DEDUP_SIZE; i++) {
        if (seen[i].occupied && now - seen[i].timestamp > TTL_SECONDS)
            seen[i].occupied = 0;
    }
}

static int is_duplicate(const char *msg_id) {
    cleanup_expired();
    unsigned int slot = hash_id(msg_id);
    if (seen[slot].occupied &&
        strncmp(seen[slot].id, msg_id, ID_MAX_LEN) == 0)
        return 1;
    strncpy(seen[slot].id, msg_id, ID_MAX_LEN - 1);
    seen[slot].id[ID_MAX_LEN - 1] = '\0';
    seen[slot].timestamp = time(NULL);
    seen[slot].occupied  = 1;
    return 0;
}

static void extract_msg_id(const char *cam_id, const char *payload,
                            char *out, size_t out_len) {
    const char *seq_start = strstr(payload, "\"seq\":");
    if (seq_start) {
        seq_start += 6;
        unsigned int seq = (unsigned int)strtoul(seq_start, NULL, 10);
        snprintf(out, out_len, "%s-e%u", cam_id, seq);
        return;
    }
    const char *ts_start = strstr(payload, "\"ts\":");
    if (ts_start) {
        ts_start += 5;
        snprintf(out, out_len, "%s-%.*s", cam_id, 12, ts_start);
    } else {
        snprintf(out, out_len, "%s-%ld", cam_id, (long)time(NULL));
    }
}

/* ── WebSocket 포트 ──────────────────────────────────────────── */
#define WS_PORT   8765

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
#define MAX_CAMS 4
typedef struct {
    char     cam_id[16];
    uint8_t *data;
    size_t   size;
} FrameCache;

static FrameCache       g_frames[MAX_CAMS];
static int              g_frame_count    = 0;
static pthread_mutex_t  g_mutex          = PTHREAD_MUTEX_INITIALIZER;
static int              g_broker_idx     = 0;
static int              g_connected      = 0;
static volatile int     g_need_failover  = 0;
static struct mosquitto *g_mosq          = NULL;

static FrameCache *get_cache(const char *cam_id) {
    for (int i = 0; i < g_frame_count; i++)
        if (strcmp(g_frames[i].cam_id, cam_id) == 0)
            return &g_frames[i];
    if (g_frame_count >= MAX_CAMS) return NULL;
    FrameCache *c = &g_frames[g_frame_count++];
    strncpy(c->cam_id, cam_id, sizeof(c->cam_id) - 1);
    c->data = NULL; c->size = 0;
    return c;
}

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
    /*
     * 캐시 프레임 즉시 전송 제거.
     * 오래된 프레임을 보내면 실시간성이 깨진다.
     * 브라우저는 다음 MQTT 프레임이 도착하는 즉시 화면을 갱신한다.
     */
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
    pthread_mutex_lock(&g_mutex);
    g_connected = 1;
    pthread_mutex_unlock(&g_mutex);

    printf("[MQTT] Connected to %s (%s)\n",
           BROKERS[g_broker_idx].host,
           BROKERS[g_broker_idx].label);

    mosquitto_subscribe(mosq, NULL, TOPIC_FRAME,  0);
    mosquitto_subscribe(mosq, NULL, TOPIC_EVENT,  2);
    mosquitto_subscribe(mosq, NULL, TOPIC_STATUS, 1);
}

static void on_mqtt_disconnect(struct mosquitto *mosq, void *ud, int rc) {
    (void)ud;
    (void)mosq;
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
     * 이를 처리하면 브라우저 화면이 계속 깜빡이므로 무시한다.
     */
    if (msg->retain) return;

    char cam_id[16];
    extract_cam_id(msg->topic, cam_id, sizeof(cam_id));

    /* ── frame 토픽 ─────────────────────────────────────────── */
    if (strstr(msg->topic, "/frame")) {
        /*
         * 8B 헤더(seq 4B LE + cam tag 4B) 기반 dedup.
         * 브로커 브리지 병렬 경로로 동일 seq가 2~3중 도달하는 것을 차단.
         * 헤더 제거 후 순수 JPEG만 WS로 전달.
         */
        if (msg->payloadlen < 8) return;

        const uint8_t *buf = (const uint8_t *)msg->payload;
        uint32_t seq;
        memcpy(&seq, buf, 4);

        char fid[ID_MAX_LEN];
        snprintf(fid, sizeof(fid), "%s-f%u", cam_id, seq);
        if (is_duplicate(fid)) return;

        const char *jpeg     = (const char *)(buf + 8);
        uint64_t    jpeg_len = (uint64_t)(msg->payloadlen - 8);

        pthread_mutex_lock(&g_mutex);
        FrameCache *c = get_cache(cam_id);
        if (c) {
            free(c->data);
            c->data = malloc((size_t)jpeg_len);
            if (c->data) {
                memcpy(c->data, jpeg, (size_t)jpeg_len);
                c->size = (size_t)jpeg_len;
            }
        }
        pthread_mutex_unlock(&g_mutex);

        ws_sendframe_bcast(WS_PORT, jpeg, jpeg_len, WS_FR_OP_BIN);
        return;
    }

    /* ── event / status 토픽: dedup 적용 ───────────────────── */
    char msg_id[ID_MAX_LEN];
    extract_msg_id(cam_id, (const char *)msg->payload,
                   msg_id, sizeof(msg_id));

    if (is_duplicate(msg_id)) {
        printf("[DEDUP] DROP  topic=%s id=%s\n", msg->topic, msg_id);
        return;
    }
    printf("[DEDUP] ACCEPT topic=%s id=%s\n", msg->topic, msg_id);

    /*
     * payload가 순수 문자열("online", "offline")이면 따옴표 추가.
     * JSON 객체({)가 아니면 브라우저에서 JSON.parse 실패.
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
        wrapper, (uint64_t)strlen(wrapper),
        WS_FR_OP_TXT);

    printf("[MQTT] %s/%s → WS: %s\n", type, cam_id, wrapper);
}

/* ─────────────────────────────────────────────────────────────
 * failover 실행
 * ───────────────────────────────────────────────────────────── */
static void do_failover(struct mosquitto *mosq)
{
    g_broker_idx = (g_broker_idx + 1) % BROKER_COUNT;
    printf("[Failover] Switching to %s (%s)\n",
           BROKERS[g_broker_idx].host,
           BROKERS[g_broker_idx].label);

    mosquitto_loop_stop(mosq, true);
    mosquitto_disconnect(mosq);

    int rc = mosquitto_connect(mosq,
                               BROKERS[g_broker_idx].host,
                               BROKERS[g_broker_idx].port,
                               60);
    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "[Failover] Connect failed to %s, will retry\n",
                BROKERS[g_broker_idx].label);
        sleep(1);
        g_need_failover = 1;
        return;
    }

    mosquitto_loop_start(mosq);
    g_need_failover = 0;
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
    g_mosq = mosquitto_new("cctv_sub_edge_c", false, NULL);
    if (!g_mosq) { perror("mosquitto_new"); return -1; }

    mosquitto_connect_callback_set(g_mosq, on_mqtt_connect);
    mosquitto_disconnect_callback_set(g_mosq, on_mqtt_disconnect);
    mosquitto_message_callback_set(g_mosq, on_mqtt_message);
    mosquitto_reconnect_delay_set(g_mosq, 1, 5, false);

    if (mosquitto_connect(g_mosq,
                          BROKERS[g_broker_idx].host,
                          BROKERS[g_broker_idx].port,
                          60) != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "[MQTT] Initial connect failed\n");
        return -1;
    }

    mosquitto_loop_start(g_mosq);

    while (1) {
        if (g_need_failover) {
            do_failover(g_mosq);
        }
        usleep(100 * 1000);
    }

    mosquitto_loop_stop(g_mosq, true);
    mosquitto_destroy(g_mosq);
    mosquitto_lib_cleanup();
    for (int i = 0; i < g_frame_count; i++)
        free(g_frames[i].data);
    return 0;
}