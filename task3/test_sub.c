/*
 * test_sub.c
 *
 * Build (macOS):
 *   brew install mosquitto
 *   gcc -Wall -O2 -o test_sub test_sub.c \
 *       -I/opt/homebrew/include \
 *       -L/opt/homebrew/lib \
 *       -lmosquitto
 *
 * 실행:
 *   ./test_sub <시나리오명>
 *   예: ./test_sub "정상"
 *       ./test_sub "단일-B1"
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <mosquitto.h>

/* ── 설정 ── */
#define SUB_BROKER   "192.168.0.29"   /* B4 */
#define SUB_PORT     1883
#define EVENT_TOTAL  100               /* 카메라 1대당 예상 수신 수 */
#define CAM_COUNT    2                 /* pi1, pi2 */
#define EXPECTED     (EVENT_TOTAL * CAM_COUNT)
#define MAX_SEQ      1000

/* ── 상태 ── */
static int   g_received[CAM_COUNT][MAX_SEQ];  /* 수신 여부 */
static int   g_recv_count[CAM_COUNT];
static int   g_max_seq[CAM_COUNT];

static double g_disconnect_t = 0;
static double g_reconnect_t  = 0;
static int    g_connected    = 0;

static char   g_scenario[64] = "unknown";
static FILE  *g_log           = NULL;

/* ── 카메라 인덱스 ── */
static int cam_index(const char *cam_id)
{
    if (strcmp(cam_id, "pi1") == 0) return 0;
    if (strcmp(cam_id, "pi2") == 0) return 1;
    return -1;
}

/* ── 현재 시각 (초, 소수점) ── */
static double now_sec(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

static char *now_str(void)
{
    static char buf[32];
    time_t t = time(NULL);
    struct tm *tm = localtime(&t);
    strftime(buf, sizeof(buf), "%H:%M:%S", tm);
    return buf;
}

/* ── cam_id 추출 ── */
static void extract_cam_id(const char *topic, char *out, size_t len)
{
    const char *p = strchr(topic, '/');
    if (!p) { strncpy(out, "unknown", len); return; }
    p++;
    const char *q = strchr(p, '/');
    if (!q) { strncpy(out, "unknown", len); return; }
    size_t n = (size_t)(q - p);
    if (n >= len) n = len - 1;
    strncpy(out, p, n);
    out[n] = '\0';
}

/* ─────────────────────────────────────────
 * MQTT 콜백
 * ───────────────────────────────────────── */
static void on_connect(struct mosquitto *mosq, void *ud, int rc)
{
    (void)ud;
    if (rc == 0) {
        g_connected = 1;

        if (g_disconnect_t > 0 && g_reconnect_t == 0) {
            g_reconnect_t = now_sec();
            double fo = g_reconnect_t - g_disconnect_t;
            printf("[%s] SUB 재연결  Failover=%.2f초\n", now_str(), fo);
            fprintf(g_log, "[%s] SUB 재연결  Failover=%.2f초\n",
                    now_str(), fo);
            fflush(g_log);
        } else {
            printf("[%s] SUB 연결됨 (%s)\n", now_str(), SUB_BROKER);
        }

        mosquitto_subscribe(mosq, NULL, "camera/+/event", 2);
    } else {
        fprintf(stderr, "[SUB] 연결 실패 rc=%d\n", rc);
    }
}

static void on_disconnect(struct mosquitto *mosq, void *ud, int rc)
{
    (void)mosq; (void)ud;
    g_connected = 0;
    if (rc != 0 && g_disconnect_t == 0) {
        g_disconnect_t = now_sec();
        printf("[%s] SUB 연결 끊김\n", now_str());
        fprintf(g_log, "[%s] SUB 연결 끊김\n", now_str());
        fflush(g_log);
    }
}

static void on_message(struct mosquitto *mosq, void *ud,
                        const struct mosquitto_message *msg)
{
    (void)mosq; (void)ud;
    if (!msg->payload) return;

    /* topic에서 cam_id 추출 */
    char cam_id[16];
    extract_cam_id(msg->topic, cam_id, sizeof(cam_id));
    int ci = cam_index(cam_id);
    if (ci < 0) return;

    /* payload에서 seq 파싱 */
    const char *p = strstr((char *)msg->payload, "\"seq\":");
    if (!p) return;
    int seq = atoi(p + 6);
    if (seq < 0 || seq >= MAX_SEQ) return;

    /* 수신 기록 */
    if (!g_received[ci][seq]) {
        g_received[ci][seq] = 1;
        g_recv_count[ci]++;
        if (seq > g_max_seq[ci]) g_max_seq[ci] = seq;
    }

    int total = g_recv_count[0] + g_recv_count[1];
    printf("[%s] RECV cam=%s seq=%03d  누계=%d/%d\n",
           now_str(), cam_id, seq, total, EXPECTED);
    fprintf(g_log, "RECV %s seq=%03d ts=%.3f\n",
            cam_id, seq, now_sec());
    fflush(g_log);
}

/* ─────────────────────────────────────────
 * 결과 출력
 * ───────────────────────────────────────── */
static void print_result(void)
{
    const char *cam_names[] = {"pi1", "pi2"};
    int total_recv = 0;
    int total_miss = 0;

    printf("\n========================================\n");
    printf("  결과: [%s]\n", g_scenario);
    printf("========================================\n");
    fprintf(g_log, "\n========================================\n");
    fprintf(g_log, "결과: [%s]\n", g_scenario);

    for (int ci = 0; ci < CAM_COUNT; ci++) {
        /* 유실 seq 찾기 */
        char missing[512] = "";
        int  miss_cnt = 0;
        for (int s = 0; s <= g_max_seq[ci]; s++) {
            if (!g_received[ci][s]) {
                char tmp[16];
                snprintf(tmp, sizeof(tmp),
                         miss_cnt == 0 ? "%d" : ",%d", s);
                if (strlen(missing) + strlen(tmp) < sizeof(missing) - 1)
                    strcat(missing, tmp);
                miss_cnt++;
            }
        }

        printf("  [%s] 수신=%d  유실=%d  유실seq=[%s]\n",
               cam_names[ci], g_recv_count[ci], miss_cnt,
               miss_cnt ? missing : "없음");
        fprintf(g_log, "[%s] 수신=%d  유실=%d  유실seq=[%s]\n",
                cam_names[ci], g_recv_count[ci], miss_cnt,
                miss_cnt ? missing : "없음");

        total_recv += g_recv_count[ci];
        total_miss += miss_cnt;
    }

    int total_expected = EXPECTED;
    double loss = total_expected > 0
        ? (1.0 - (double)total_recv / total_expected) * 100.0
        : 100.0;

    double fo = (g_disconnect_t > 0 && g_reconnect_t > 0)
        ? g_reconnect_t - g_disconnect_t : -1;

    printf("  ──────────────────────────────────\n");
    printf("  예상=%d  수신=%d  유실률=%.1f%%\n",
           total_expected, total_recv, loss);
    printf("  Failover: %s\n",
           fo >= 0 ? (char[32]){0} : "측정 없음");
    if (fo >= 0) printf("  Failover: %.2f초\n", fo);
    printf("========================================\n\n");

    fprintf(g_log, "예상=%d  수신=%d  유실률=%.1f%%\n",
            total_expected, total_recv, loss);
    if (fo >= 0)
        fprintf(g_log, "Failover=%.2f초\n", fo);
    fprintf(g_log, "========================================\n\n");
    fflush(g_log);
}

/* ─────────────────────────────────────────
 * 상태 초기화
 * ───────────────────────────────────────── */
static void reset_state(void)
{
    memset(g_received,  0, sizeof(g_received));
    memset(g_recv_count, 0, sizeof(g_recv_count));
    memset(g_max_seq,    0, sizeof(g_max_seq));
    g_disconnect_t = 0;
    g_reconnect_t  = 0;
}

/* ─────────────────────────────────────────
 * main
 * ───────────────────────────────────────── */
int main(int argc, char *argv[])
{
    if (argc < 2) {
        fprintf(stderr, "사용법: ./test_sub <시나리오명>\n");
        fprintf(stderr, "예시:   ./test_sub 정상\n");
        fprintf(stderr, "        ./test_sub 단일-B1\n");
        return 1;
    }
    strncpy(g_scenario, argv[1], sizeof(g_scenario) - 1);

    /* 로그 파일 */
    char logname[128];
    time_t t = time(NULL);
    struct tm *tm = localtime(&t);
    strftime(logname, sizeof(logname),
             "test_%Y%m%d_%H%M%S.log", tm);
    g_log = fopen(logname, "w");
    if (!g_log) { perror("fopen"); return 1; }

    fprintf(g_log, "시나리오: %s\n", g_scenario);
    fprintf(g_log, "시작: %s\n\n", now_str());

    printf("========================================\n");
    printf("  CCTV Test Subscriber\n");
    printf("  시나리오: %s\n", g_scenario);
    printf("  로그: %s\n", logname);
    printf("========================================\n");

    /* MQTT 초기화 */
    mosquitto_lib_init();
    struct mosquitto *mosq = mosquitto_new("test_sub", true, NULL);
    if (!mosq) { perror("mosquitto_new"); return 1; }

    mosquitto_connect_callback_set(mosq, on_connect);
    mosquitto_disconnect_callback_set(mosq, on_disconnect);
    mosquitto_message_callback_set(mosq, on_message);

    if (mosquitto_connect(mosq, SUB_BROKER, SUB_PORT, 60) != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "브로커 연결 실패\n");
        return 1;
    }

    /*
     * 안내 출력
     * 사람이 직접 보면서 진행
     */
    printf("\n  [준비]\n");
    printf("  1. Pub1, Pub2 에서 ./publisher 실행\n");
    printf("  2. 브로커 정상 동작 확인\n");
    printf("  3. 이벤트 발송 시작하면 수신 로그 출력됨\n");
    if (strcmp(g_scenario, "정상") != 0) {
        printf("\n  [장애 주입 타이밍]\n");
        printf("  수신 누계 %d개 (30%%) 도달 시 브로커 중지\n",
               (int)(EXPECTED * 0.3));
    }
    printf("\n  Ctrl+C 로 종료 및 결과 출력\n");
    printf("----------------------------------------\n\n");

    /* 메인 루프 */
    mosquitto_loop_forever(mosq, -1, 1);

    /* Ctrl+C 시 결과 출력 */
    print_result();

    fclose(g_log);
    mosquitto_destroy(mosq);
    mosquitto_lib_cleanup();
    return 0;
}