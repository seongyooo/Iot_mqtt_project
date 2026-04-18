/*
 * publisher.c
 *
 * Build:
 *   gcc -Wall -O2 -o publisher publisher.c -lmosquitto -lsqlite3 -lpthread
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

/* ── 브로커 failover 목록 ────────────────────────────────────── */
typedef struct {
    const char *host;
    int         port;
    const char *label;
} broker_t;

static const broker_t BROKERS[] = {
    { "192.168.0.7",  1883, "B1" },
    { "192.168.0.11", 1883, "B2" },
    { "192.168.0.13", 1883, "B3" },
    { "192.168.0.9",  1883, "B4" },
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
static int              g_broker_idx         = 0;
static int              g_connected          = 0;
static volatile int     g_need_failover      = 0;
static volatile int     g_failover_in_progress = 0; /* do_failover 실행 중 on_disconnect 억제용 */
static time_t           g_connect_start      = 0;   /* 연결 시도 시각 — TCP 타임아웃 감지용 */
#define CONNECT_TIMEOUT_SEC 3                   /* N초 내 on_connect 없으면 다음 브로커로 */
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
        g_failover_in_progress = 0;  /* 실제 연결 성공 시점에 해제 — stale on_disconnect 억제 종료 */
        pthread_mutex_lock(&g_mutex);
        g_connected = 1;
        pthread_mutex_unlock(&g_mutex);
        g_connect_start = 0;  /* 연결 성공 — 타이머 해제 */
        printf("[MQTT] Connected to %s (%s)\n",
               BROKERS[g_broker_idx].host, BROKERS[g_broker_idx].label);
        mosquitto_publish(mosq, NULL, TOPIC_STATUS,
                          strlen("online"), "online", 1, true);
        db_flush(mosq);
    } else {
        fprintf(stderr, "[MQTT] Connect failed rc=%d\n", rc);
    }
}

/*
 * ⚠️  콜백은 libmosquitto 내부 네트워크 스레드에서 실행된다.
 *     deadlock 방지를 위해 플래그만 세우고
 *     실제 재연결은 메인 루프에서 처리한다.
 */
static void on_disconnect(struct mosquitto *mosq, void *ud, int rc)
{
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
         * 여기서 다시 1로 세우면 B3 연결 틈도 없이 즉시 다음 브로커로 skip된다.
         */
        if (g_failover_in_progress) return;
        printf("[MQTT] Disconnected from %s. Triggering failover...\n",
               BROKERS[g_broker_idx].label);
        g_need_failover = 1;
    }
}

/* ─────────────────────────────────────────────────────────────
 * failover — disconnect → connect_async
 *
 * ✅  loop_stop 제거: 이전 방식(loop_stop true)은 내부 스레드가
 *     완전히 종료될 때까지 블로킹 → keepalive 60초 설정 시
 *     죽은 브로커마다 최대 60초 대기 → 2분 이상 지연 발생.
 *
 *     loop_start는 mqtt_init에서 1회만 호출하고 계속 실행 중이므로
 *     재호출 불필요. disconnect 후 connect_async만 하면
 *     네트워크 스레드가 비동기로 연결을 처리한다.
 * ───────────────────────────────────────────────────────────── */
static void do_failover(struct mosquitto *mosq)
{
    g_broker_idx = (g_broker_idx + 1) % BROKER_COUNT;
    printf("[Failover] Switching to %s (%s)\n",
           BROKERS[g_broker_idx].host, BROKERS[g_broker_idx].label);

    /*
     * in_progress 플래그를 먼저 세운다:
     * mosquitto_disconnect()가 on_disconnect(rc != 0)를 비동기 호출하더라도
     * 그 콜백이 g_need_failover를 다시 1로 세우지 않게 막는다.
     * (B2가 TCP 미연결 상태로 끊길 때 race condition 방지)
     */
    g_failover_in_progress = 1;
    mosquitto_disconnect(mosq);
    g_need_failover = 0;          /* connect_async 전에 클리어 */
    g_connect_start = time(NULL); /* 연결 시도 시각 기록 */

    int rc = mosquitto_connect_async(mosq,
                                     BROKERS[g_broker_idx].host,
                                     BROKERS[g_broker_idx].port, 10); /* keepalive 10 */
    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "[Failover] connect_async to %s rc=%d, will retry\n",
                BROKERS[g_broker_idx].label, rc);
        g_connect_start = 0;
        g_need_failover = 1;
    }
    /* loop_start 재호출 없음 — mqtt_init에서 1회 시작해 계속 실행 중 */
    /* g_failover_in_progress는 on_connect에서 해제 —
     * 여기서 해제하면 stale on_disconnect가 플래그 해제 후 도착해
     * g_need_failover를 다시 세울 수 있음 (race condition) */
}

/* ─────────────────────────────────────────────────────────────
 * MQTT 초기화
 * ───────────────────────────────────────────────────────────── */
static struct mosquitto *mqtt_init(void)
{
    mosquitto_lib_init();
    struct mosquitto *mosq = mosquitto_new("cctv_pub_" CAM_ID, false, NULL);
    if (!mosq) { perror("mosquitto_new"); return NULL; }
    mosquitto_connect_callback_set(mosq, on_connect);
    mosquitto_disconnect_callback_set(mosq, on_disconnect);
    /* mosquitto_reconnect_delay_set 제거 — loop_start 모드에서 자동 재연결이
     * 수동 failover와 타이밍 충돌 가능. 재연결은 TCP timeout 루프로만 관리. */
    mosquitto_will_set(mosq, TOPIC_STATUS, strlen("offline"), "offline", 1, true);

    /*
     * connect_async: 비동기 연결 — 브로커가 꺼져있어도 즉시 리턴.
     * 연결 성공/실패는 on_connect / on_disconnect 콜백으로 통보된다.
     * 실패 시 g_need_failover = 1로 메인 루프가 재시도한다.
     */
    for (int i = 0; i < BROKER_COUNT; i++) {
        int rc = mosquitto_connect_async(mosq, BROKERS[i].host, BROKERS[i].port, 10);
        if (rc == MOSQ_ERR_SUCCESS) {
            g_broker_idx    = i;
            g_connect_start = time(NULL); /* 연결 시도 시각 기록 */
            printf("[MQTT] Initial connect_async to %s (%s)\n",
                   BROKERS[i].host, BROKERS[i].label);
            break;
        }
        fprintf(stderr, "[MQTT] %s (%s) connect_async failed rc=%d\n",
                BROKERS[i].host, BROKERS[i].label, rc);
    }

    /* loop_start: 네트워크 스레드 1회 시작 — 이후 재호출 없음 */
    mosquitto_loop_start(mosq);
    return mosq;
}

/* ─────────────────────────────────────────────────────────────
 * 이벤트 발행 — QoS 2 + SQLite fallback
 * ───────────────────────────────────────────────────────────── */
static void publish_event(struct mosquitto *mosq, const char *payload)
{
    db_enqueue(TOPIC_EVENT, payload);
    pthread_mutex_lock(&g_mutex);
    int connected = g_connected;
    pthread_mutex_unlock(&g_mutex);
    if (connected) db_flush(mosq);
}

/* ─────────────────────────────────────────────────────────────
 * rpicam-vid 파이프
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

/* ─────────────────────────────────────────────────────────────
 * MJPEG 스트림 → JPEG 프레임 1개 파싱
 *   SOI: 0xFF 0xD8 / EOI: 0xFF 0xD9
 * ───────────────────────────────────────────────────────────── */
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

/* ─────────────────────────────────────────────────────────────
 * 모션 감지 — 연속 프레임 임계값 방식
 *
 * JPEG 크기 ∝ 화면 복잡도. 움직임 → 압축률 저하 → 크기 증가.
 * MOTION_CONFIRM_FRAMES 연속으로 임계값 초과 시에만 모션으로 확정해
 * 조명 변화 등 1프레임 노이즈에 의한 오감지를 방지한다.
 * ───────────────────────────────────────────────────────────── */
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
    g_mosq = mqtt_init();
    if (!g_mosq) return -1;

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
        /* failover 처리 — 플래그 확인 후 do_failover() 호출 */
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

        size_t   fsize = 0;
        uint8_t *frame = read_jpeg_frame(pipe, &fsize);
        if (!frame) {
            fprintf(stderr, "[Camera] Pipe broken, restarting...\n");
            pclose(pipe); sleep(2); goto retry;
        }

        /* 프레임 발행 — QoS 0, 순수 JPEG (헤더 없음) */
        pthread_mutex_lock(&g_mutex);
        int connected = g_connected;
        pthread_mutex_unlock(&g_mutex);

        if (connected)
            mosquitto_publish(g_mosq, NULL, TOPIC_FRAME,
                              (int)fsize, frame, 0, false);

        /* 모션 감지 → 이벤트 발행 QoS 2 */
        if (detect_motion(fsize, prev_size)) {
            char payload[128];
            snprintf(payload, sizeof(payload),
                     "{\"cam\":\"%s\",\"event\":\"motion\",\"ts\":%ld}",
                     CAM_ID, (long)time(NULL));
            publish_event(g_mosq, payload);
            printf("[Event] Motion detected  frame=%zu bytes\n", fsize);
        }
        prev_size = fsize;

        /* FPS 출력 (10초마다) */
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
    mosquitto_loop_stop(g_mosq, true);
    mosquitto_destroy(g_mosq);
    mosquitto_lib_cleanup();
    sqlite3_close(g_db);
    return 0;
}