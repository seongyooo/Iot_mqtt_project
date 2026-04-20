/*
 * publisher.c
 *
 * Raspberry Pi CCTV 퍼블리셔.
 * rpicam-vid 파이프에서 MJPEG 프레임을 캡처해 MQTT 브로커로 발행한다.
 * 연결된 브로커가 끊기면 다음 브로커로 자동 페일오버한다.
 *
 * 모션 감지는 연속 JPEG 프레임의 크기 비율로 판단한다.
 * 임계값 이상의 크기 증가가 일정 프레임 수 연속되면 모션 이벤트를 발행한다.
 *
 * 토픽 구조 (subscriber_ws.c가 브릿지를 통해 수신):
 *   camera/<CAM_ID>/frame   - JPEG 바이너리, QoS 0
 *   camera/<CAM_ID>/event   - JSON 모션 이벤트, QoS 2
 *   camera/<CAM_ID>/status  - "online" / "offline" (retained), QoS 1
 *
 * 빌드:
 *   gcc -Wall -O2 -o publisher publisher.c -lmosquitto -lpthread
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include <mosquitto.h>

/* -----------------------------------------------------------------
 * 카메라 ID
 * 장비마다 다르게 설정한다 (예: "pi1", "pi2").
 * ----------------------------------------------------------------- */
#define CAM_ID  "pi1"

/* -----------------------------------------------------------------
 * MQTT 토픽
 * ----------------------------------------------------------------- */
#define TOPIC_FRAME   "camera/" CAM_ID "/frame"
#define TOPIC_EVENT   "camera/" CAM_ID "/event"
#define TOPIC_STATUS  "camera/" CAM_ID "/status"

/* -----------------------------------------------------------------
 * 브로커 페일오버 목록
 * 퍼블리셔는 Edge 브로커(B1, B2)에만 연결한다.
 * ----------------------------------------------------------------- */
typedef struct {
    const char *host;
    int         port;
    const char *label;
} broker_t;

static const broker_t BROKERS[] = {
    { "192.168.0.7",  1883, "B1" },
    { "192.168.0.11", 1883, "B2" },
};
#define BROKER_COUNT (int)(sizeof(BROKERS) / sizeof(BROKERS[0]))

/* -----------------------------------------------------------------
 * 카메라 캡처 설정
 * ----------------------------------------------------------------- */
#define CAM_WIDTH    640
#define CAM_HEIGHT   480
#define CAM_FPS      15
#define CAM_QUALITY  80   /* JPEG 품질 0-100 */

/* -----------------------------------------------------------------
 * 프레임 버퍼 크기
 * ----------------------------------------------------------------- */
#define PIPE_CHUNK   (4 * 1024)     /* fread() 1회 읽기 단위 */
#define FRAME_MAX    (512 * 1024)   /* JPEG 프레임 최대 크기 */

/* -----------------------------------------------------------------
 * 모션 감지 파라미터
 * 프레임 크기 비율이 MOTION_RATIO_THRESHOLD를 넘는 상태가
 * MOTION_CONFIRM_FRAMES 프레임 연속되면 모션 이벤트를 발행한다.
 * ----------------------------------------------------------------- */
#define MOTION_RATIO_THRESHOLD  1.20
#define MOTION_CONFIRM_FRAMES   1

/* -----------------------------------------------------------------
 * 연결 타임아웃
 * TCP 핸드셰이크가 이 시간 내에 완료되지 않으면 다음 브로커로 전환한다.
 * ----------------------------------------------------------------- */
#define CONNECT_TIMEOUT_SEC  3

/* -----------------------------------------------------------------
 * 전역 MQTT 상태
 * ----------------------------------------------------------------- */
static int              g_broker_idx    = 0;
static int              g_connected     = 0;
static volatile int     g_need_failover = 0;
static time_t           g_connect_start = 0;
static pthread_mutex_t  g_mutex         = PTHREAD_MUTEX_INITIALIZER;
static struct mosquitto *g_mosq         = NULL;

/* -----------------------------------------------------------------
 * on_connect 콜백
 * 브로커 연결 수락 시 mosquitto 네트워크 스레드에서 호출된다.
 * retained "online" 상태를 발행하고 페일오버 플래그를 초기화한다.
 * ----------------------------------------------------------------- */
static void on_connect(struct mosquitto *mosq, void *ud, int rc)
{
    (void)ud;
    if (rc == 0) {
        g_need_failover = 0;
        pthread_mutex_lock(&g_mutex);
        g_connected = 1;
        pthread_mutex_unlock(&g_mutex);
        g_connect_start = 0;

        printf("[MQTT] 연결됨: %s (%s)\n",
               BROKERS[g_broker_idx].host, BROKERS[g_broker_idx].label);

        mosquitto_publish(mosq, NULL, TOPIC_STATUS,
                          (int)strlen("online"), "online", 1, true);
    } else {
        fprintf(stderr, "[MQTT] 연결 실패 rc=%d\n", rc);
    }
}

/* -----------------------------------------------------------------
 * on_disconnect 콜백
 * 연결이 예기치 않게 끊겼을 때(rc != 0) 페일오버 플래그를 세운다.
 * ----------------------------------------------------------------- */
static void on_disconnect(struct mosquitto *mosq, void *ud, int rc)
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

/* -----------------------------------------------------------------
 * setup_callbacks
 * MQTT 이벤트 콜백을 등록하고 LWT를 설정한다.
 * 클라이언트가 비정상 종료하면 브로커가 "offline"을 자동 발행한다.
 * ----------------------------------------------------------------- */
static void setup_callbacks(struct mosquitto *mosq)
{
    mosquitto_connect_callback_set(mosq, on_connect);
    mosquitto_disconnect_callback_set(mosq, on_disconnect);
    mosquitto_will_set(mosq, TOPIC_STATUS,
                       (int)strlen("offline"), "offline", 1, true);
}

/* -----------------------------------------------------------------
 * mosq_stop
 * mosquitto 네트워크 스레드를 강제 종료한다.
 * connect_async가 TCP 핸드셰이크 중일 때는 mosquitto_disconnect()가
 * MOSQ_ERR_NO_CONN을 반환해 소켓을 닫지 않는다.
 * loop_stop(true)로 pthread_cancel을 통해 즉시 종료를 보장한다.
 * ----------------------------------------------------------------- */
static void mosq_stop(struct mosquitto *mosq)
{
    mosquitto_disconnect(mosq);
    mosquitto_loop_stop(mosq, true);
}

/* -----------------------------------------------------------------
 * do_failover
 * 현재 mosquitto 인스턴스를 파괴하고 다음 브로커로 새 인스턴스를 생성한다.
 * 브로커 목록을 라운드로빈 방식으로 순환한다.
 * ----------------------------------------------------------------- */
static void do_failover(void)
{
    g_broker_idx = (g_broker_idx + 1) % BROKER_COUNT;
    printf("[Failover] %s (%s) 으로 전환\n",
           BROKERS[g_broker_idx].host, BROKERS[g_broker_idx].label);

    mosq_stop(g_mosq);
    mosquitto_destroy(g_mosq);

    g_mosq = mosquitto_new("cctv_pub_" CAM_ID, true, NULL);
    if (!g_mosq) {
        perror("[Failover] mosquitto_new");
        g_need_failover = 1;
        return;
    }

    setup_callbacks(g_mosq);
    g_need_failover  = 0;
    g_connect_start  = time(NULL);

    int rc = mosquitto_connect_async(g_mosq,
                                     BROKERS[g_broker_idx].host,
                                     BROKERS[g_broker_idx].port, 10);
    if (rc != MOSQ_ERR_SUCCESS) {
        fprintf(stderr, "[Failover] connect_async 실패: %s rc=%d (%s)\n",
                BROKERS[g_broker_idx].label, rc, mosquitto_strerror(rc));
        g_connect_start = 0;
        g_need_failover = 1;
        return;
    }

    mosquitto_loop_start(g_mosq);
}

/* -----------------------------------------------------------------
 * mqtt_init
 * mosquitto 라이브러리를 초기화하고 첫 번째 가용 브로커에
 * 비동기 연결을 시도한다.
 * ----------------------------------------------------------------- */
static struct mosquitto *mqtt_init(void)
{
    mosquitto_lib_init();

    struct mosquitto *mosq = mosquitto_new("cctv_pub_" CAM_ID, true, NULL);
    if (!mosq) {
        perror("mosquitto_new");
        return NULL;
    }

    setup_callbacks(mosq);

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
        fprintf(stderr, "[MQTT] %s (%s) connect_async 실패 rc=%d (%s)\n",
                BROKERS[i].host, BROKERS[i].label, rc, mosquitto_strerror(rc));
    }

    mosquitto_loop_start(mosq);
    return mosq;
}

/* -----------------------------------------------------------------
 * publish_event
 * QoS 2로 모션 이벤트를 발행한다.
 * 미연결 상태에서는 이벤트를 드롭하고 경고를 출력한다.
 * ----------------------------------------------------------------- */
static void publish_event(struct mosquitto *mosq, const char *payload)
{
    pthread_mutex_lock(&g_mutex);
    int connected = g_connected;
    pthread_mutex_unlock(&g_mutex);

    if (!connected) {
        fprintf(stderr, "[Event] 미연결 상태, 이벤트 드롭: %s\n", payload);
        return;
    }

    mosquitto_publish(mosq, NULL, TOPIC_EVENT,
                      (int)strlen(payload), payload, 2, false);
}

/* -----------------------------------------------------------------
 * open_camera_pipe
 * rpicam-vid를 자식 프로세스로 실행하고 MJPEG 스트림을 읽을
 * FILE* 를 반환한다.
 * ----------------------------------------------------------------- */
static FILE *open_camera_pipe(void)
{
    char cmd[256];
    snprintf(cmd, sizeof(cmd),
             "rpicam-vid -t 0 --codec mjpeg"
             " --width %d --height %d --framerate %d -q %d"
             " --nopreview -o - 2>/dev/null",
             CAM_WIDTH, CAM_HEIGHT, CAM_FPS, CAM_QUALITY);

    FILE *p = popen(cmd, "r");
    if (!p)
        fprintf(stderr, "[Camera] popen 실패\n");
    return p;
}

/* -----------------------------------------------------------------
 * read_jpeg_frame
 * MJPEG 파이프를 읽어 완전한 JPEG 프레임 1장을 반환한다.
 * SOI(0xFF 0xD8)와 EOI(0xFF 0xD9) 마커로 프레임 경계를 찾는다.
 * 반환된 버퍼는 호출자가 free() 해야 한다.
 * 파이프 오류 시 NULL을 반환한다.
 * ----------------------------------------------------------------- */
static uint8_t *read_jpeg_frame(FILE *pipe, size_t *out_size)
{
    static uint8_t buf[FRAME_MAX];
    static size_t  buf_len = 0;
    static uint8_t chunk[PIPE_CHUNK];

    while (1) {
        size_t n = fread(chunk, 1, sizeof(chunk), pipe);
        if (n == 0)
            return NULL;

        /* 버퍼 오버플로우 방지: 넘치면 초기화 */
        if (buf_len + n > FRAME_MAX)
            buf_len = 0;

        memcpy(buf + buf_len, chunk, n);
        buf_len += n;

        /* SOI 마커 탐색 */
        size_t soi = SIZE_MAX;
        for (size_t i = 0; i + 1 < buf_len; i++) {
            if (buf[i] == 0xFF && buf[i + 1] == 0xD8) {
                soi = i;
                break;
            }
        }
        if (soi == SIZE_MAX) {
            buf_len = 0;
            continue;
        }
        if (soi > 0) {
            memmove(buf, buf + soi, buf_len - soi);
            buf_len -= soi;
        }

        /* EOI 마커 탐색 후 완전한 프레임 반환 */
        for (size_t i = 2; i + 1 < buf_len; i++) {
            if (buf[i] == 0xFF && buf[i + 1] == 0xD9) {
                size_t   flen  = i + 2;
                uint8_t *frame = malloc(flen);
                if (!frame)
                    return NULL;
                memcpy(frame, buf, flen);
                *out_size = flen;
                memmove(buf, buf + flen, buf_len - flen);
                buf_len -= flen;
                return frame;
            }
        }
    }
}

/* -----------------------------------------------------------------
 * detect_motion
 * 모션 이벤트 발행 여부를 반환한다 (1: 발행, 0: 대기).
 * 프레임 크기 비율이 임계값을 초과하는 상태가 연속되어야 확정된다.
 * ----------------------------------------------------------------- */
static int detect_motion(size_t cur, size_t prev)
{
    static int consecutive_count = 0;

    if (prev == 0) {
        consecutive_count = 0;
        return 0;
    }

    double ratio = (double)cur / (double)prev;

    if (ratio > MOTION_RATIO_THRESHOLD) {
        consecutive_count++;
        printf("[Motion] ratio=%.2f  count=%d/%d\n",
               ratio, consecutive_count, MOTION_CONFIRM_FRAMES);

        if (consecutive_count >= MOTION_CONFIRM_FRAMES) {
            consecutive_count = 0;
            return 1;
        }
    } else {
        if (consecutive_count > 0)
            printf("[Motion] 초기화 (ratio=%.2f)\n", ratio);
        consecutive_count = 0;
    }

    return 0;
}

/* -----------------------------------------------------------------
 * main
 * ----------------------------------------------------------------- */
int main(void)
{
    g_mosq = mqtt_init();
    if (!g_mosq)
        return 1;

    printf("[Publisher] CAM_ID=%s  %dx%d @%dfps\n",
           CAM_ID, CAM_WIDTH, CAM_HEIGHT, CAM_FPS);

    size_t prev_size  = 0;
    int    frame_cnt  = 0;
    time_t last_fps_t = time(NULL);

retry:;
    FILE *pipe = open_camera_pipe();
    if (!pipe) {
        sleep(3);
        goto retry;
    }
    printf("[Camera] 파이프 열림\n");

    while (1) {
        /* 페일오버 처리 */
        if (g_need_failover)
            do_failover();

        /* TCP 타임아웃 시 페일오버 트리거 */
        if (!g_connected && g_connect_start > 0 &&
            time(NULL) - g_connect_start > CONNECT_TIMEOUT_SEC) {
            printf("[Failover] %s TCP 타임아웃, 다음 브로커로 전환\n",
                   BROKERS[g_broker_idx].label);
            g_connect_start = 0;
            g_need_failover = 1;
        }

        /* 카메라 파이프에서 JPEG 프레임 1장 읽기 */
        size_t   fsize = 0;
        uint8_t *frame = read_jpeg_frame(pipe, &fsize);
        if (!frame) {
            fprintf(stderr, "[Camera] 파이프 끊김, 재시작\n");
            pclose(pipe);
            sleep(2);
            goto retry;
        }

        /* 연결 상태일 때만 프레임 발행 (QoS 0, non-retained) */
        pthread_mutex_lock(&g_mutex);
        int connected = g_connected;
        pthread_mutex_unlock(&g_mutex);

        if (connected)
            mosquitto_publish(g_mosq, NULL, TOPIC_FRAME,
                              (int)fsize, frame, 0, false);

        /* 모션 감지 후 이벤트 발행 */
        if (detect_motion(fsize, prev_size)) {
            char payload[128];
            snprintf(payload, sizeof(payload),
                     "{\"cam\":\"%s\",\"event\":\"motion\",\"ts\":%ld}",
                     CAM_ID, (long)time(NULL));
            publish_event(g_mosq, payload);
            printf("[Event] 모션 감지  frame=%zu bytes\n", fsize);
        }

        prev_size = fsize;
        frame_cnt++;

        /* 10초마다 FPS 통계 출력 */
        time_t now = time(NULL);
        if (now - last_fps_t >= 10) {
            printf("[Stats] %.1f fps  broker=%s\n",
                   (double)frame_cnt / (double)(now - last_fps_t),
                   BROKERS[g_broker_idx].label);
            frame_cnt  = 0;
            last_fps_t = now;
        }

        free(frame);
    }

    pclose(pipe);
    mosq_stop(g_mosq);
    mosquitto_destroy(g_mosq);
    mosquitto_lib_cleanup();
    return 0;
}