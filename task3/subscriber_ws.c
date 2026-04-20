/*
 * subscriber_ws.c
 *
 * MQTT 서브스크라이버 + WebSocket 브릿지.
 * Core 브로커(B3, B4)에 연결해 카메라 데이터를 수신하고,
 * WebSocket을 통해 브라우저 클라이언트에 실시간으로 전달한다.
 *
 * 메시지 라우팅:
 *   MQTT 토픽 형식: <edge>/<core>/camera/<cam_id>/<type>
 *     예) b1/b3/camera/pi1/frame
 *   경로 정보를 파싱해 브로커 활성 상태를 추적하고,
 *   경로 변경 시 브라우저에 알림을 전송한다.
 *
 * WebSocket으로 전송하는 메시지 타입:
 *   Binary  - 카메라 프레임: [1B: cam_id 길이][cam_id][JPEG 바이너리]
 *   Text/JSON:
 *     { type: "route",         cam, from, via }
 *     { type: "broker_status", broker, active }
 *     { type: "sub_connected", broker }
 *     { type: "event",         cam, from, via, data }
 *     { type: "status",        cam, from, via, data }
 *
 * 빌드:
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

/* -----------------------------------------------------------------
 * 설정값
 * ----------------------------------------------------------------- */
#define WS_PORT             8765
#define CONNECT_TIMEOUT_SEC    3   /* TCP 페일오버 타임아웃 (초) */
#define BROKER_TIMEOUT_SEC     5   /* 수신 없으면 inactive 처리 (초) */

/* -----------------------------------------------------------------
 * Core 브로커 목록
 * 서브스크라이버는 Core 브로커(B3, B4)에만 연결한다.
 * Edge 브로커(B1, B2)의 트래픽은 MQTT 브릿지를 통해 여기로 전달된다.
 * ----------------------------------------------------------------- */
typedef struct { const char *host; int port; const char *label; } broker_t;

static const broker_t BROKERS[] = {
    { "192.168.0.13", 1883, "B3" },
    { "192.168.0.29", 1883, "B4" },
};
#define BROKER_COUNT (int)(sizeof(BROKERS) / sizeof(BROKERS[0]))

/* 모든 카메라 토픽 구독 (edge/core 경로 포함) */
#define TOPIC_ALL "+/+/camera/#"

/* -----------------------------------------------------------------
 * 전역 MQTT 상태
 * ----------------------------------------------------------------- */
static int              g_broker_idx    = 0;
static int              g_connected     = 0;
static volatile int     g_need_failover = 0;
static time_t           g_connect_start = 0;
static struct mosquitto *g_mosq         = NULL;
static pthread_mutex_t  g_mutex         = PTHREAD_MUTEX_INITIALIZER;

/* -----------------------------------------------------------------
 * 카메라별 라우팅 상태
 * 각 카메라의 마지막 edge/core 브로커 쌍을 저장한다.
 * 경로가 변경되면 WebSocket 클라이언트에 브로드캐스트한다.
 * ----------------------------------------------------------------- */
#define MAX_CAMS 10

typedef struct {
    char cam_id[16];
    char last_from[8];   /* 마지막 Edge 브로커 (예: "b1") */
    char last_via[8];    /* 마지막 Core 브로커 (예: "b3") */
} cam_status_t;

static cam_status_t    g_cam_status[MAX_CAMS];
static int             g_cam_count  = 0;
static pthread_mutex_t g_cam_mutex  = PTHREAD_MUTEX_INITIALIZER;

/* -----------------------------------------------------------------
 * 브로커별 마지막 수신 시각
 * 인덱스: b1=0, b2=1, b3=2, b4=3
 * watchdog 스레드가 이 값으로 inactive 상태를 감지한다.
 * ----------------------------------------------------------------- */
static time_t          g_broker_seen[4] = { 0 };
static pthread_mutex_t g_broker_mutex   = PTHREAD_MUTEX_INITIALIZER;

/* -----------------------------------------------------------------
 * broker_to_idx
 * 브로커 레이블을 g_broker_seen[] 인덱스로 변환한다.
 * 알 수 없는 레이블은 -1을 반환한다.
 * ----------------------------------------------------------------- */
static int broker_to_idx(const char *name)
{
    if (strcasecmp(name, "b1") == 0) return 0;
    if (strcasecmp(name, "b2") == 0) return 1;
    if (strcasecmp(name, "b3") == 0) return 2;
    if (strcasecmp(name, "b4") == 0) return 3;
    return -1;
}

/* -----------------------------------------------------------------
 * parse_topic
 * "<edge>/<core>/camera/<cam>/<type>" 형식의 토픽을 파싱한다.
 * 성공 시 0, 형식 불일치 시 -1을 반환한다.
 * ----------------------------------------------------------------- */
static int parse_topic(const char *topic,
                       char *from, char *via,
                       char *cam,  char *type)
{
    return (sscanf(topic, "%7[^/]/%7[^/]/camera/%15[^/]/%15s",
                   from, via, cam, type) == 4) ? 0 : -1;
}

/* -----------------------------------------------------------------
 * update_route
 * 카메라의 라우팅 경로를 갱신한다.
 * 경로가 변경된 경우 모든 WebSocket 클라이언트에 JSON을 브로드캐스트한다.
 * ----------------------------------------------------------------- */
static void update_route(const char *cam_id,
                         const char *from, const char *via)
{
    pthread_mutex_lock(&g_cam_mutex);

    /* 해당 카메라 슬롯을 찾거나 새로 생성 */
    int ci = -1;
    for (int i = 0; i < g_cam_count; i++) {
        if (strcmp(g_cam_status[i].cam_id, cam_id) == 0) {
            ci = i;
            break;
        }
    }
    if (ci < 0 && g_cam_count < MAX_CAMS) {
        ci = g_cam_count++;
        strncpy(g_cam_status[ci].cam_id, cam_id, sizeof(g_cam_status[ci].cam_id) - 1);
        g_cam_status[ci].last_from[0] = '\0';
        g_cam_status[ci].last_via[0]  = '\0';
    }
    if (ci < 0) {
        pthread_mutex_unlock(&g_cam_mutex);
        return;
    }

    int changed = (strcmp(g_cam_status[ci].last_from, from) != 0 ||
                   strcmp(g_cam_status[ci].last_via,  via)  != 0);

    if (changed) {
        printf("[Route] cam=%s: %s->%s => %s->%s\n",
               cam_id,
               g_cam_status[ci].last_from[0] ? g_cam_status[ci].last_from : "?",
               g_cam_status[ci].last_via[0]  ? g_cam_status[ci].last_via  : "?",
               from, via);

        strncpy(g_cam_status[ci].last_from, from, sizeof(g_cam_status[ci].last_from) - 1);
        strncpy(g_cam_status[ci].last_via,  via,  sizeof(g_cam_status[ci].last_via)  - 1);

        char json[256];
        snprintf(json, sizeof(json),
                 "{\"type\":\"route\",\"cam\":\"%s\","
                 "\"from\":\"%s\",\"via\":\"%s\"}",
                 cam_id, from, via);
        ws_sendframe_bcast(WS_PORT, json, strlen(json), WS_FR_OP_TXT);
    }

    pthread_mutex_unlock(&g_cam_mutex);
}

/* -----------------------------------------------------------------
 * broker_watchdog (스레드)
 * 2초마다 g_broker_seen[]을 확인해 타임아웃된 브로커를 감지하고,
 * 활성/비활성 상태가 바뀌면 WebSocket 클라이언트에 알린다.
 * ----------------------------------------------------------------- */
static void *broker_watchdog(void *arg)
{
    (void)arg;
    const char *names[]  = { "b1", "b2", "b3", "b4" };
    int         prev[4]  = { -1, -1, -1, -1 };

    while (1) {
        sleep(2);

        time_t now = time(NULL);
        pthread_mutex_lock(&g_broker_mutex);

        for (int i = 0; i < 4; i++) {
            if (g_broker_seen[i] == 0)
                continue;

            int active = (now - g_broker_seen[i]) < BROKER_TIMEOUT_SEC;

            if (active != prev[i]) {
                prev[i] = active;

                char json[128];
                snprintf(json, sizeof(json),
                         "{\"type\":\"broker_status\","
                         "\"broker\":\"%s\",\"active\":%s}",
                         names[i], active ? "true" : "false");
                ws_sendframe_bcast(WS_PORT, json, strlen(json), WS_FR_OP_TXT);

                printf("[Broker] %s => %s\n",
                       names[i], active ? "active" : "inactive");
            }
        }

        pthread_mutex_unlock(&g_broker_mutex);
    }
    return NULL;
}

/* -----------------------------------------------------------------
 * send_current_state
 * 특정 WebSocket 클라이언트에게 현재 라우팅/브로커 상태를 전송한다.
 * 신규 연결 시 및 request_state 수신 시 호출된다.
 * ----------------------------------------------------------------- */
static void send_current_state(ws_cli_conn_t client)
{
    /* 각 카메라의 현재 라우팅 경로 */
    pthread_mutex_lock(&g_cam_mutex);
    for (int i = 0; i < g_cam_count; i++) {
        if (!g_cam_status[i].last_from[0])
            continue;
        char json[256];
        snprintf(json, sizeof(json),
                 "{\"type\":\"route\",\"cam\":\"%s\","
                 "\"from\":\"%s\",\"via\":\"%s\"}",
                 g_cam_status[i].cam_id,
                 g_cam_status[i].last_from,
                 g_cam_status[i].last_via);
        ws_sendframe(client, json, strlen(json), WS_FR_OP_TXT);
    }
    pthread_mutex_unlock(&g_cam_mutex);

    /* 추적 중인 브로커의 활성 상태 */
    const char *names[] = { "b1", "b2", "b3", "b4" };
    pthread_mutex_lock(&g_broker_mutex);
    time_t now = time(NULL);
    for (int i = 0; i < 4; i++) {
        if (g_broker_seen[i] == 0)
            continue;
        int active = (now - g_broker_seen[i]) < BROKER_TIMEOUT_SEC;
        char json[128];
        snprintf(json, sizeof(json),
                 "{\"type\":\"broker_status\","
                 "\"broker\":\"%s\",\"active\":%s}",
                 names[i], active ? "true" : "false");
        ws_sendframe(client, json, strlen(json), WS_FR_OP_TXT);
    }
    pthread_mutex_unlock(&g_broker_mutex);

    /* 현재 연결된 Core 브로커 정보 */
    char json[128];
    snprintf(json, sizeof(json),
             "{\"type\":\"sub_connected\",\"broker\":\"%s\"}",
             BROKERS[g_broker_idx].label);
    ws_sendframe(client, json, strlen(json), WS_FR_OP_TXT);
}

/* -----------------------------------------------------------------
 * WebSocket 이벤트 콜백
 * ----------------------------------------------------------------- */
void onopen(ws_cli_conn_t client)
{
    printf("[WS] 클라이언트 연결: %s\n", ws_getaddress(client));
    send_current_state(client);
}

void onclose(ws_cli_conn_t client)
{
    printf("[WS] 클라이언트 연결 해제: %s\n", ws_getaddress(client));
}

void onmessage(ws_cli_conn_t client,
               const unsigned char *msg, uint64_t size, int type)
{
    (void)size; (void)type;
    if (!msg)
        return;

    /* 브라우저가 연결 후 상태 동기화를 요청할 때 전송 */
    if (strncmp((const char *)msg, "request_state", 13) == 0)
        send_current_state(client);
}

/* -----------------------------------------------------------------
 * MQTT 이벤트 콜백
 * ----------------------------------------------------------------- */
static void on_mqtt_connect(struct mosquitto *mosq, void *ud, int rc)
{
    (void)ud;
    if (rc != 0) {
        fprintf(stderr, "[MQTT] 연결 실패 rc=%d\n", rc);
        g_need_failover = 1;
        return;
    }

    g_need_failover = 0;
    pthread_mutex_lock(&g_mutex);
    g_connected = 1;
    pthread_mutex_unlock(&g_mutex);
    g_connect_start = 0;

    printf("[MQTT] 연결됨: %s (%s)\n",
           BROKERS[g_broker_idx].host, BROKERS[g_broker_idx].label);

    mosquitto_subscribe(mosq, NULL, TOPIC_ALL, 2);

    /* 연결된 Core 브로커를 브라우저에 알림 */
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
        printf("[MQTT] 연결 끊김: %s -> 페일오버 시작\n",
               BROKERS[g_broker_idx].label);
        g_need_failover = 1;
    }
}

static void on_mqtt_message(struct mosquitto *mosq, void *ud,
                            const struct mosquitto_message *msg)
{
    (void)mosq; (void)ud;
    if (!msg->payload || msg->payloadlen <= 0)
        return;
    if (msg->retain)   /* retained 메시지 무시 */
        return;

    /* 구조화된 토픽 파싱 */
    char from[8], via[8], cam_id[16], sub_type[16];
    if (parse_topic(msg->topic, from, via, cam_id, sub_type) != 0)
        return;

    /* 경로 상 두 브로커의 마지막 수신 시각 갱신 */
    pthread_mutex_lock(&g_broker_mutex);
    int fi = broker_to_idx(from);
    int vi = broker_to_idx(via);
    if (fi >= 0) g_broker_seen[fi] = time(NULL);
    if (vi >= 0) g_broker_seen[vi] = time(NULL);
    pthread_mutex_unlock(&g_broker_mutex);

    /* 라우팅 경로 변경 감지 및 알림 */
    update_route(cam_id, from, via);

    if (strcmp(sub_type, "frame") == 0) {
        /*
         * WebSocket 바이너리 프레임 포맷:
         *   [1바이트: cam_id 길이][cam_id 바이트][JPEG 바이트]
         * JSON 헤더 파싱 없이 브라우저가 카메라를 식별할 수 있도록 한다.
         */
        uint8_t  id_len = (uint8_t)strlen(cam_id);
        size_t   total  = 1 + id_len + (size_t)msg->payloadlen;
        uint8_t *buf    = malloc(total);
        if (!buf)
            return;

        buf[0] = id_len;
        memcpy(buf + 1,          cam_id,       id_len);
        memcpy(buf + 1 + id_len, msg->payload, msg->payloadlen);

        ws_sendframe_bcast(WS_PORT, (const char *)buf,
                           (uint64_t)total, WS_FR_OP_BIN);
        free(buf);

    } else {
        /*
         * 이벤트/상태 메시지는 JSON 래퍼로 감싸서 전송한다.
         * 원본 payload가 JSON이면 객체로, 아니면 문자열로 삽입한다.
         */
        char        wrapper[1024];
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
        printf("[MQTT] %s/%s via %s->%s\n", type, cam_id, from, via);
    }
}

/* -----------------------------------------------------------------
 * setup_callbacks
 * mosquitto 인스턴스에 MQTT 콜백을 등록한다.
 * ----------------------------------------------------------------- */
static void setup_callbacks(struct mosquitto *mosq)
{
    mosquitto_connect_callback_set(mosq, on_mqtt_connect);
    mosquitto_disconnect_callback_set(mosq, on_mqtt_disconnect);
    mosquitto_message_callback_set(mosq, on_mqtt_message);
}

/* -----------------------------------------------------------------
 * mosq_stop
 * mosquitto 네트워크 스레드를 강제 종료한다.
 * publisher.c의 mosq_stop과 동일한 이유로 loop_stop(true)를 사용한다.
 * ----------------------------------------------------------------- */
static void mosq_stop(struct mosquitto *mosq)
{
    mosquitto_disconnect(mosq);
    mosquitto_loop_stop(mosq, true);
}

/* -----------------------------------------------------------------
 * do_failover
 * 다음 Core 브로커로 전환한다 (라운드로빈).
 * ----------------------------------------------------------------- */
static void do_failover(void)
{
    g_broker_idx = (g_broker_idx + 1) % BROKER_COUNT;
    printf("[Failover] %s (%s) 으로 전환\n",
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

    /*
     * 일부 플랫폼(libmosquitto 2.x)에서 connect_async 이전에
     * loop_start를 먼저 호출해야 MOSQ_ERR_NO_CONN 오류를 방지할 수 있다.
     */
    mosquitto_loop_start(g_mosq);

    int rc = mosquitto_connect_async(g_mosq,
                                     BROKERS[g_broker_idx].host,
                                     BROKERS[g_broker_idx].port, 10);
    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "[Failover] connect_async 실패 rc=%d (%s)\n",
                rc, mosquitto_strerror(rc));
        g_connect_start = 0;
        g_need_failover = 1;
    }
}

/* -----------------------------------------------------------------
 * mqtt_init
 * mosquitto를 초기화하고 첫 번째 가용 Core 브로커에 연결한다.
 * ----------------------------------------------------------------- */
static struct mosquitto *mqtt_init(void)
{
    mosquitto_lib_init();

    struct mosquitto *mosq = mosquitto_new("cctv_sub_edge_c", true, NULL);
    if (!mosq) {
        perror("mosquitto_new");
        return NULL;
    }

    setup_callbacks(mosq);

    /* do_failover 주석 참고: loop_start를 connect_async 이전에 호출 */
    mosquitto_loop_start(mosq);

    for (int i = 0; i < BROKER_COUNT; i++) {
        int rc = mosquitto_connect_async(mosq,
                                         BROKERS[i].host, BROKERS[i].port, 10);
        if (rc == MOSQ_ERR_SUCCESS) {
            g_broker_idx    = i;
            g_connect_start = time(NULL);
            printf("[MQTT] 초기 connect_async: %s (%s)\n",
                   BROKERS[i].host, BROKERS[i].label);
            break;
        }
    }

    return mosq;
}

/* -----------------------------------------------------------------
 * main
 * ----------------------------------------------------------------- */
int main(void)
{
    /* WebSocket 서버를 백그라운드 스레드로 시작 */
    ws_socket(&(struct ws_server){
        .host        = "0.0.0.0",
        .port        = WS_PORT,
        .thread_loop = 1,
        .timeout_ms  = 1000,
        .evs.onopen    = &onopen,
        .evs.onclose   = &onclose,
        .evs.onmessage = &onmessage,
    });
    printf("[WS] 서버 시작: 포트 %d\n", WS_PORT);

    /* 브로커 watchdog 스레드 시작 */
    pthread_t watchdog_tid;
    pthread_create(&watchdog_tid, NULL, broker_watchdog, NULL);
    pthread_detach(watchdog_tid);

    /* MQTT 연결 */
    g_mosq = mqtt_init();
    if (!g_mosq)
        return 1;

    /* 메인 루프: 페일오버 및 연결 타임아웃 처리 */
    while (1) {
        if (g_need_failover) {
            do_failover();
            continue;
        }

        if (!g_connected && g_connect_start > 0 &&
            time(NULL) - g_connect_start > CONNECT_TIMEOUT_SEC) {
            g_connect_start = 0;
            g_need_failover = 1;
        }

        usleep(100000);   /* 100ms 폴링 간격 */
    }

    mosq_stop(g_mosq);
    mosquitto_destroy(g_mosq);
    mosquitto_lib_cleanup();
    return 0;
}