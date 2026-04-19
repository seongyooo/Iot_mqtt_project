/*
 * subscriber_ws_fixed.c
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <mosquitto.h>
#include <wsserver/ws.h>

/* ── WebSocket ───────────────── */
#define WS_PORT 8765

/* ── Broker list ─────────────── */
typedef struct {
    const char *host;
    int port;
    const char *label;
} broker_t;

// static const broker_t BROKERS[] = {
//     { "192.168.0.13", 1883, "B3" },
//     { "192.168.0.9",  1883, "B4" },
//     { "192.168.0.7",  1883, "B1" },
//     { "192.168.0.11", 1883, "B2" },
// };
static const broker_t BROKERS[] = {
    { "192.168.0.29", 1883, "B3" },
    { "192.168.0.8",  1883, "B4" }
    // { "192.168.0.7",  1883, "B1" },
    // { "192.168.0.11", 1883, "B2" },
};
#define BROKER_COUNT (int)(sizeof(BROKERS)/sizeof(BROKERS[0]))

/* ── Topics ──────────────────── */
#define TOPIC_FRAME   "camera/+/frame"
#define TOPIC_EVENT   "camera/+/event"
#define TOPIC_STATUS  "camera/+/status"

/* ── State ───────────────────── */
static int g_broker_idx = 0;
static int g_connected = 0;
static int g_need_failover = 0;
static time_t g_connect_start = 0;

static int g_loop_count = 0;  // ⭐ 핵심 추가

#define CONNECT_TIMEOUT_SEC 3
#define CONNECT_GRACE_LOOPS 5   // ⭐ 최소 loop 보장

static struct mosquitto *g_mosq = NULL;

/* ───────────────────────────── */

static void setup_callbacks(struct mosquitto *mosq);

/* ── MQTT Callbacks ─────────── */

static void on_connect(struct mosquitto *mosq, void *ud, int rc) {
    if (rc != 0) {
        printf("[MQTT] Connect failed rc=%d\n", rc);
        return;
    }

    g_connected = 1;
    g_need_failover = 0;
    g_connect_start = 0;
    g_loop_count = 0;

    printf("[MQTT] Connected to %s (%s)\n",
           BROKERS[g_broker_idx].host,
           BROKERS[g_broker_idx].label);

    mosquitto_subscribe(mosq, NULL, TOPIC_FRAME, 0);
    mosquitto_subscribe(mosq, NULL, TOPIC_EVENT, 2);
    mosquitto_subscribe(mosq, NULL, TOPIC_STATUS, 1);
}

static void on_disconnect(struct mosquitto *mosq, void *ud, int rc) {
    g_connected = 0;

    if (rc != 0) {
        printf("[MQTT] Unexpected disconnect → failover\n");
        g_need_failover = 1;
    }
}

static void on_message(struct mosquitto *mosq, void *ud,
                       const struct mosquitto_message *msg) {
    if (!msg->payload) return;

    if (strstr(msg->topic, "/frame")) {
        ws_sendframe_bcast(WS_PORT,
            (const char *)msg->payload,
            msg->payloadlen,
            WS_FR_OP_BIN);
        return;
    }

    printf("[MSG] %.*s\n", (int)msg->payloadlen, (char *)msg->payload);
}

/* ── WebSocket Callbacks ───── */

static void on_ws_open(ws_cli_conn_t c) {
    (void)c;
    printf("[WS] client connected\n");
}

static void on_ws_close(ws_cli_conn_t c) {
    (void)c;
    printf("[WS] client disconnected\n");
}

static void on_ws_msg(ws_cli_conn_t c,
                      const unsigned char *msg, uint64_t sz, int type) {
    (void)c; (void)msg; (void)sz; (void)type;
}

/* ── Callback setup ─────────── */

static void setup_callbacks(struct mosquitto *mosq) {
    mosquitto_connect_callback_set(mosq, on_connect);
    mosquitto_disconnect_callback_set(mosq, on_disconnect);
    mosquitto_message_callback_set(mosq, on_message);
}

/* ── Failover ───────────────── */

static void do_failover(void) {
    g_broker_idx = (g_broker_idx + 1) % BROKER_COUNT;

    printf("[Failover] → %s (%s)\n",
           BROKERS[g_broker_idx].host,
           BROKERS[g_broker_idx].label);

    mosquitto_destroy(g_mosq);

    g_mosq = mosquitto_new("sub_client", true, NULL);
    if (!g_mosq) {
        perror("mosquitto_new");
        g_need_failover = 1;
        return;
    }

    setup_callbacks(g_mosq);

    g_connected = 0;
    g_need_failover = 0;
    g_connect_start = time(NULL);
    g_loop_count = 0;

    int rc = mosquitto_connect(
        g_mosq,
        BROKERS[g_broker_idx].host,
        BROKERS[g_broker_idx].port,
        10
    );

    if (rc != MOSQ_ERR_SUCCESS) {
        printf("[Failover] connect_async failed\n");
        g_need_failover = 1;
    }
}

/* ── MAIN ───────────────────── */

int main(void) {

    /* WebSocket */
    ws_socket(&(struct ws_server){
        .host = "0.0.0.0",
        .port = WS_PORT,
        .thread_loop = 1,
        .timeout_ms = 1000,
        .evs = {
            .onopen    = on_ws_open,
            .onclose   = on_ws_close,
            .onmessage = on_ws_msg,
        }
    });

    printf("[WS] Started on %d\n", WS_PORT);

    mosquitto_lib_init();

    g_mosq = mosquitto_new("sub_client", true, NULL);
    if (!g_mosq) return -1;

    setup_callbacks(g_mosq);

    mosquitto_connect_async(
        g_mosq,
        BROKERS[g_broker_idx].host,
        BROKERS[g_broker_idx].port,
        10
    );

    g_connect_start = time(NULL);

    /* ── LOOP ───────────────── */

    while (1) {

        /* failover */
        if (g_need_failover) {
            do_failover();
        }

        /* ⭐ 핵심: loop 항상 실행 */
        int rc = mosquitto_loop(g_mosq, 100, 1);
        g_loop_count++;

        /* timeout (grace 이후만) */
        if (!g_connected &&
            g_connect_start > 0 &&
            g_loop_count > CONNECT_GRACE_LOOPS &&
            time(NULL) - g_connect_start > CONNECT_TIMEOUT_SEC) {

            printf("[Timeout] switching broker\n");
            g_need_failover = 1;
        }

        if (rc != MOSQ_ERR_SUCCESS &&
            rc != MOSQ_ERR_NO_CONN) {

            printf("[Loop error] rc=%d → failover\n", rc);
            g_need_failover = 1;
        }
    }

    return 0;
}