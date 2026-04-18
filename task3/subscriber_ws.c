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
static pthread_mutex_t  g_mutex              = PTHREAD_MUTEX_INITIALIZER;
static int              g_broker_idx         = 0;
static int              g_connected          = 0;
static volatile int     g_need_failover      = 0;
static volatile int     g_failover_in_progress = 0; /* do_failover 실행 중 on_disconnect 억제용 */
static time_t           g_connect_start      = 0;   /* 연결 시도 시각 — TCP 타임아웃 감지용 */
#define CONNECT_TIMEOUT_SEC 3                        /* N초 내 on_connect 없으면 다음 브로커로 */
static struct mosquitto *g_mosq              = NULL;

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
    if (rc != 0) {
        fprintf(stderr, "[MQTT] Connect failed rc=%d\n", rc);
        return;
    }
    g_failover_in_progress = 0;  /* 실제 연결 성공 시점에 해제 — stale on_disconnect 억제 종료 */
    pthread_mutex_lock(&g_mutex);
    g_connected = 1;
    pthread_mutex_unlock(&g_mutex);
    g_connect_start = 0;  /* 연결 성공 — 타이머 해제 */
    printf("[MQTT] Connected to %s (%s)\n",
           BROKERS[g_broker_idx].host, BROKERS[g_broker_idx].label);
    /* 재연결 시에도 on_connect가 호출되므로 여기서 구독 등록 */
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
        /*
         * g_failover_in_progress 중일 때는 무시:
         * do_failover() 안에서 의도적으로 mosquitto_disconnect()를 호출하면
         * 네트워크 스레드가 rc != 0으로 이 콜백을 비동기 호출한다.
         * 그 시점에 메인 스레드가 이미 g_need_failover = 0으로 클리어했어도
         * 여기서 다시 1로 세우면 다음 브로커 연결 틈도 없이 즉시 skip된다.
         */
        if (g_failover_in_progress) return;
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

    /* ── frame: 순수 JPEG → binary broadcast ── */
    if (strstr(msg->topic, "/frame")) {
        ws_sendframe_bcast(WS_PORT,
            (const char *)msg->payload,
            (uint64_t)msg->payloadlen,
            WS_FR_OP_BIN);
        return;
    }

    /* ── event / status: JSON 래퍼 후 text broadcast ── */
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
 * failover — disconnect → connect_async
 *
 * ✅  loop_stop 제거: 이전 방식(loop_stop true)은 내부 스레드가
 *     완전히 종료될 때까지 블로킹 → keepalive 60초 설정 시
 *     죽은 브로커마다 최대 60초 대기 → 2분 이상 지연 발생.
 *
 *     loop_start는 main에서 1회만 호출하고 계속 실행 중이므로
 *     재호출 불필요. disconnect 후 connect_async만 하면
 *     네트워크 스레드가 비동기로 연결을 처리한다.
 * ───────────────────────────────────────────────────────────── */
static void do_failover(struct mosquitto *mosq) {
    g_broker_idx = (g_broker_idx + 1) % BROKER_COUNT;
    printf("[Failover] Switching to %s (%s)\n",
           BROKERS[g_broker_idx].host, BROKERS[g_broker_idx].label);

    /*
     * in_progress 플래그를 먼저 세운다:
     * mosquitto_disconnect()가 on_disconnect(rc != 0)를 비동기 호출하더라도
     * 그 콜백이 g_need_failover를 다시 1로 세우지 않게 막는다.
     * (이전 브로커가 TCP 미연결 상태로 끊길 때 race condition 방지)
     */
    g_failover_in_progress = 1;
    mosquitto_disconnect(mosq);
    g_need_failover = 0;          /* connect_async 전에 클리어 */
    g_connect_start = time(NULL); /* 연결 시도 시각 기록 */

    int rc = mosquitto_connect_async(mosq,
                                     BROKERS[g_broker_idx].host,
                                     BROKERS[g_broker_idx].port, 10);
    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "[Failover] connect_async to %s rc=%d, will retry\n",
                BROKERS[g_broker_idx].label, rc);
        g_connect_start = 0;
        g_need_failover = 1;
    }
    /* loop_start 재호출 없음 — main에서 1회 시작해 계속 실행 중 */
    /* g_failover_in_progress는 on_mqtt_connect에서 해제 —
     * 여기서 해제하면 stale on_disconnect가 플래그 해제 후 도착해
     * g_need_failover를 다시 세울 수 있음 (race condition) */
}

/* ─────────────────────────────────────────────────────────────
 * main
 * ───────────────────────────────────────────────────────────── */
int main(void) {
    /* WebSocket 서버 — 백그라운드 스레드 */
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
    /* mosquitto_reconnect_delay_set 제거 — loop_start 모드에서 자동 재연결이
     * 수동 failover와 타이밍 충돌 가능. 재연결은 TCP timeout 루프로만 관리. */

    /*
     * connect_async: 비동기 연결 — 브로커가 꺼져있어도 즉시 리턴.
     * 연결 성공/실패는 on_connect / on_disconnect 콜백으로 통보된다.
     * 실패 시 g_need_failover = 1로 메인 루프가 재시도한다.
     */
    int rc = mosquitto_connect_async(g_mosq,
                                     BROKERS[g_broker_idx].host,
                                     BROKERS[g_broker_idx].port, 10);
    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "[MQTT] Initial connect_async rc=%d, failover will retry\n", rc);
        g_broker_idx = BROKER_COUNT - 1;
        g_need_failover = 1;
    } else {
        printf("[MQTT] Initial connect_async to %s (%s)\n",
               BROKERS[0].host, BROKERS[0].label);
        g_connect_start = time(NULL);  /* 연결 시도 시각 기록 */
    }

    /* loop_start: 네트워크 스레드 1회 시작 — 이후 재호출 없음 */
    mosquitto_loop_start(g_mosq);

    /*
     * 수동 메인 루프 — 100ms마다 failover 플래그 확인
     */
    while (1) {
        if (g_need_failover) do_failover(g_mosq);

        /*
         * TCP 연결 타임아웃 감지
         * Linux 기본 TCP SYN 재전송이 최대 127초이므로
         * CONNECT_TIMEOUT_SEC 내에 on_connect가 없으면 강제로 다음 브로커로 전환.
         */
        if (!g_connected && g_connect_start > 0 &&
            time(NULL) - g_connect_start > CONNECT_TIMEOUT_SEC) {
            printf("[Failover] TCP timeout on %s, switching next broker\n",
                   BROKERS[g_broker_idx].label);
            g_connect_start = 0;
            g_need_failover = 1;
        }

        usleep(100 * 1000);
    }

    mosquitto_loop_stop(g_mosq, true);
    mosquitto_destroy(g_mosq);
    mosquitto_lib_cleanup();
    return 0;
}