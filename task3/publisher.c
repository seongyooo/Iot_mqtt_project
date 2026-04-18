/*
 * publisher.c
 *
 * Build:
 * gcc -Wall -O2 -o publisher publisher.c -lmosquitto -lsqlite3 -lpthread
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include <sqlite3.h>
#include <mosquitto.h>

/* ── 카메라 ID ───────────────────────────────────────────────── */
#define CAM_ID  "pi1"   /* publisher 장비마다 변경 */

/* ── 토픽 ────────────────────────────────────────────────────── */
#define TOPIC_FRAME   "camera/" CAM_ID "/frame"
#define TOPIC_EVENT   "camera/" CAM_ID "/event"
#define TOPIC_STATUS  "camera/" CAM_ID "/status"

/* ── 브로커 failover 목록 (블랙리스트 기능 추가) ────────────────*/
typedef struct {
    const char *host;
    int         port;
    const char *label;
    time_t      dead_until;  /* 이 시간 이전에는 무조건 스킵 */
} broker_t;

static broker_t BROKERS[] = {
    { "192.168.0.7",  1883, "B1", 0 },
    { "192.168.0.11", 1883, "B2", 0 },
    { "192.168.0.13", 1883, "B3", 0 },
    { "192.168.0.9",  1883, "B4", 0 },
};
#define BROKER_COUNT (int)(sizeof(BROKERS)/sizeof(BROKERS[0]))

/* ── 카메라 설정 ─────────────────────────────────────────────── */
#define CAM_WIDTH    640
#define CAM_HEIGHT   480
#define CAM_FPS      15
#define CAM_QUALITY  80

/* ── 프레임 버퍼 ─────────────────────────────────────────────── */
#define PIPE_CHUNK   (4 * 1024)
#define FRAME_MAX    (512 * 1024)

/* ── SQLite 경로 ─────────────────────────────────────────────── */
#define DB_PATH  "/tmp/event_queue.db"

/* ── 모션 감지 설정 ──────────────────────────────────────────── */
#define MOTION_RATIO_THRESHOLD  1.20
#define MOTION_CONFIRM_FRAMES   3

/* ─────────────────────────────────────────────────────────────
 * 전역 상태
 * ───────────────────────────────────────────────────────────── */
static int              g_broker_idx    = 0;
static int              g_connected     = 0;
static volatile int     g_need_failover = 1; /* 초기 시작 시 failover 타게 함 */
static pthread_mutex_t  g_mutex         = PTHREAD_MUTEX_INITIALIZER;
static struct mosquitto *g_mosq         = NULL;
static sqlite3         *g_db            = NULL;

/* ─────────────────────────────────────────────────────────────
 * SQLite 큐 — 브로커 전부 다운 시 이벤트 로컬 저장
 * ───────────────────────────────────────────────────────────── */
static int db_init(void)
{
    if (sqlite3_open(DB_PATH, &g_db) != SQLITE_OK) {
        fprintf(stderr, "[DB] Open failed: %s\n", sqlite3_errmsg(g_db));
        return -1;
    }
    const char *sql =
        "CREATE TABLE IF NOT EXISTS queue ("
        "  id      INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  topic   TEXT    NOT NULL,"
        "  payload TEXT    NOT NULL,"
        "  ts      INTEGER NOT NULL,"
        "  sent    INTEGER DEFAULT 0);";
    char *err = NULL;
    sqlite3_exec(g_db, sql, NULL, NULL, &err);
    if (err) { fprintf(stderr, "[DB] %s\n", err); sqlite3_free(err); return -1; }
    return 0;
}

static void db_enqueue(const char *topic, const char *payload)
{
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(g_db,
        "INSERT INTO queue (topic,payload,ts) VALUES(?,?,?);", -1, &s, NULL);
    sqlite3_bind_text(s, 1, topic,   -1, SQLITE_STATIC);
    sqlite3_bind_text(s, 2, payload, -1, SQLITE_STATIC);
    sqlite3_bind_int64(s, 3, (sqlite3_int64)time(NULL));
    sqlite3_step(s);
    sqlite3_finalize(s);
    printf("[DB] Queued: %s\n", payload);
}

static void db_flush(struct mosquitto *mosq)
{
    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(g_db,
        "SELECT id,topic,payload FROM queue WHERE sent=0 ORDER BY id LIMIT 50;",
        -1, &s, NULL);
    while (sqlite3_step(s) == SQLITE_ROW) {
        int         id      = sqlite3_column_int(s, 0);
        const char *topic   = (const char *)sqlite3_column_text(s, 1);
        const char *payload = (const char *)sqlite3_column_text(s, 2);
        int rc = mosquitto_publish(mosq, NULL, topic,
                                   (int)strlen(payload), payload, 2, false);
        if (rc == MOSQ_ERR_SUCCESS) {
            char upd[64];
            snprintf(upd, sizeof(upd), "UPDATE queue SET sent=1 WHERE id=%d;", id);
            sqlite3_exec(g_db, upd, NULL, NULL, NULL);
            printf("[DB] Flushed id=%d\n", id);
        } else break;
    }
    sqlite3_finalize(s);
}

/* ─────────────────────────────────────────────────────────────
 * MQTT 콜백
 * ───────────────────────────────────────────────────────────── */
static void on_connect(struct mosquitto *mosq, void *ud, int rc)
{
    (void)ud;
    if (rc == 0) {
        pthread_mutex_lock(&g_mutex);
        g_connected = 1;
        pthread_mutex_unlock(&g_mutex);
        printf("[MQTT] Connected to %s (%s)\n",
               BROKERS[g_broker_idx].host, BROKERS[g_broker_idx].label);
        mosquitto_publish(mosq, NULL, TOPIC_STATUS,
                          strlen("online"), "online", 1, true);
        db_flush(mosq);
    } else {
        fprintf(stderr, "[MQTT] Connect failed rc=%d\n", rc);
    }
}

static void on_disconnect(struct mosquitto *mosq, void *ud, int rc)
{
    (void)ud; (void)mosq;
    pthread_mutex_lock(&g_mutex);
    g_connected = 0;
    pthread_mutex_unlock(&g_mutex);
    
    if (rc != 0) {
        printf("[MQTT] Disconnected from %s. Triggering failover...\n",
               BROKERS[g_broker_idx].label);
        
        /* 백그라운드 연결 실패 시 해당 브로커에 60초 페널티 부여 */
        BROKERS[g_broker_idx].dead_until = time(NULL) + 60;
        g_need_failover = 1;
    }
}

/* ─────────────────────────────────────────────────────────────
 * 무중단 Failover 처리 (객체 재성성 + 비동기 + 블랙리스트 + Keepalive 단축)
 * ───────────────────────────────────────────────────────────── */
static void do_failover(void)
{
    pthread_mutex_lock(&g_mutex);
    g_connected = 0;
    pthread_mutex_unlock(&g_mutex);

    /* 1. 꼬여있을 수 있는 기존 연결 객체 완전 파괴 */
    if (g_mosq) {
        mosquitto_loop_stop(g_mosq, true);
        mosquitto_destroy(g_mosq);
    }

    /* 2. 깨끗한 새 객체 생성 */
    g_mosq = mosquitto_new("cctv_pub_" CAM_ID, false, NULL);
    if (!g_mosq) { perror("mosquitto_new"); return; }
    
    mosquitto_connect_callback_set(g_mosq, on_connect);
    mosquitto_disconnect_callback_set(g_mosq, on_disconnect);
    mosquitto_reconnect_delay_set(g_mosq, 1, 5, false);
    mosquitto_will_set(g_mosq, TOPIC_STATUS, strlen("offline"), "offline", 1, true);

    time_t now = time(NULL);
    int connected = 0;

    for (int i = 0; i < BROKER_COUNT; i++) {
        g_broker_idx = (g_broker_idx + 1) % BROKER_COUNT;
        
        /* 3. 블랙리스트 검사: 페널티 기간이면 즉시 스킵 (0초 소요) */
        if (now < BROKERS[g_broker_idx].dead_until) {
            printf("[Failover] Skipping %s (Dead until %ld)\n", 
                   BROKERS[g_broker_idx].label, BROKERS[g_broker_idx].dead_until - now);
            continue; 
        }

        printf("[Failover] Trying %s (%s)...\n", 
               BROKERS[g_broker_idx].host, BROKERS[g_broker_idx].label);

        /* 4. 비동기 연결 (Keepalive 5초로 단축하여 반좀비 상태 방지) */
        int rc = mosquitto_connect_async(g_mosq, 
                                         BROKERS[g_broker_idx].host, 
                                         BROKERS[g_broker_idx].port, 5);
                                         
        if (rc == MOSQ_ERR_SUCCESS) {
            connected = 1;
            break; 
        } else {
            /* 문법/시스템 레벨 에러 발생 시 즉시 블랙리스트 추가 */
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
        g_need_failover = 1; /* 메인 루프에서 계속 재시도 유도 */
    }
}

/* ─────────────────────────────────────────────────────────────
 * 이벤트 발행 — QoS 2 + SQLite fallback
 * ───────────────────────────────────────────────────────────── */
static void publish_event(const char *payload)
{
    db_enqueue(TOPIC_EVENT, payload);
    pthread_mutex_lock(&g_mutex);
    int connected = g_connected;
    pthread_mutex_unlock(&g_mutex);
    if (connected && g_mosq) db_flush(g_mosq);
}

/* ─────────────────────────────────────────────────────────────
 * rpicam-vid 파이프 & 카메라 처리
 * ───────────────────────────────────────────────────────────── */
static FILE *open_camera_pipe(void)
{
    char cmd[256];
    snprintf(cmd, sizeof(cmd),
        "rpicam-vid -t 0 --codec mjpeg"
        " --width %d --height %d --framerate %d -q %d"
        " --nopreview -o - 2>/dev/null",
        CAM_WIDTH, CAM_HEIGHT, CAM_FPS, CAM_QUALITY);
    FILE *p = popen(cmd, "r");
    if (!p) fprintf(stderr, "[Camera] popen failed\n");
    return p;
}

static uint8_t *read_jpeg_frame(FILE *pipe, size_t *out_size)
{
    static uint8_t buf[FRAME_MAX];
    static size_t  buf_len = 0;
    static uint8_t chunk[PIPE_CHUNK];
    while (1) {
        size_t n = fread(chunk, 1, sizeof(chunk), pipe);
        if (n == 0) return NULL;
        if (buf_len + n > FRAME_MAX) buf_len = 0;
        memcpy(buf + buf_len, chunk, n);
        buf_len += n;
        size_t soi = SIZE_MAX;
        for (size_t i = 0; i + 1 < buf_len; i++)
            if (buf[i] == 0xFF && buf[i+1] == 0xD8) { soi = i; break; }
        if (soi == SIZE_MAX) { buf_len = 0; continue; }
        if (soi > 0) { memmove(buf, buf + soi, buf_len - soi); buf_len -= soi; }
        for (size_t i = 2; i + 1 < buf_len; i++) {
            if (buf[i] == 0xFF && buf[i+1] == 0xD9) {
                size_t flen = i + 2;
                uint8_t *frame = malloc(flen);
                if (!frame) return NULL;
                memcpy(frame, buf, flen);
                *out_size = flen;
                memmove(buf, buf + flen, buf_len - flen);
                buf_len -= flen;
                return frame;
            }
        }
    }
}

static int detect_motion(size_t cur, size_t prev)
{
    static int consecutive_count = 0;
    if (prev == 0) { consecutive_count = 0; return 0; }
    double ratio = (double)cur / (double)prev;
    if (ratio > MOTION_RATIO_THRESHOLD) {
        consecutive_count++;
        printf("[Motion] ratio=%.2f count=%d/%d\n",
               ratio, consecutive_count, MOTION_CONFIRM_FRAMES);
        if (consecutive_count >= MOTION_CONFIRM_FRAMES) {
            consecutive_count = 0;
            return 1;
        }
    } else {
        if (consecutive_count > 0)
            printf("[Motion] Reset (ratio=%.2f)\n", ratio);
        consecutive_count = 0;
    }
    return 0;
}

/* ─────────────────────────────────────────────────────────────
 * main
 * ───────────────────────────────────────────────────────────── */
int main(void)
{
    if (db_init() != 0) return -1;
    mosquitto_lib_init(); /* 라이브러리 초기화 한 번 수행 */

    printf("[Publisher] CAM_ID=%s  %dx%d @%dfps\n",
           CAM_ID, CAM_WIDTH, CAM_HEIGHT, CAM_FPS);

    size_t prev_size  = 0;
    int    frame_cnt  = 0;
    time_t last_fps_t = time(NULL);

retry:;
    FILE *pipe = open_camera_pipe();
    if (!pipe) { sleep(3); goto retry; }
    printf("[Camera] Pipe opened\n");

    while (1) {
        /* 파라미터 없이 호출 (함수 내부에서 전역 g_mosq 교체) */
        if (g_need_failover) do_failover();

        size_t   fsize = 0;
        uint8_t *frame = read_jpeg_frame(pipe, &fsize);
        if (!frame) {
            fprintf(stderr, "[Camera] Pipe broken, restarting...\n");
            pclose(pipe); sleep(2); goto retry;
        }

        pthread_mutex_lock(&g_mutex);
        int connected = g_connected;
        pthread_mutex_unlock(&g_mutex);

        if (connected && g_mosq && !g_need_failover)
            mosquitto_publish(g_mosq, NULL, TOPIC_FRAME,
                              (int)fsize, frame, 0, false);

        if (detect_motion(fsize, prev_size)) {
            char payload[128];
            snprintf(payload, sizeof(payload),
                     "{\"cam\":\"%s\",\"event\":\"motion\",\"ts\":%ld}",
                     CAM_ID, (long)time(NULL));
            publish_event(payload);
            printf("[Event] Motion detected  frame=%zu bytes\n", fsize);
        }
        prev_size = fsize;

        frame_cnt++;
        time_t now = time(NULL);
        if (now - last_fps_t >= 10) {
            printf("[Stats] %.1f fps  broker=%s\n",
                   (double)frame_cnt / (double)(now - last_fps_t),
                   BROKERS[g_broker_idx].label);
            frame_cnt = 0; last_fps_t = now;
        }
        free(frame);
    }

    pclose(pipe);
    if (g_mosq) {
        mosquitto_loop_stop(g_mosq, true);
        mosquitto_destroy(g_mosq);
    }
    mosquitto_lib_cleanup();
    sqlite3_close(g_db);
    return 0;
}