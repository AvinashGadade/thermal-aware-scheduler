#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static double now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

static int run_cmd(const char *cmd) {
    int rc = system(cmd);
    if (rc != 0) {
        fprintf(stderr, "\n[ERROR] Command failed (rc=%d):\n%s\n\n", rc, cmd);
    }
    return rc;
}

/*
 * Read max CPU temp from /sys/class/thermal/thermal_zone*/temp.
 * Many kernels report milli-degC (e.g., 45000), so convert to degC.
 * Returns -1.0 if not available.
 *
 * NOTE: This reads "thermal zones" which might be CPU/package/skin depending on laptop.
 */
static double read_temp_c_local(void) {
    const char *cmd =
        "sh -lc \""
        "max=-1; maxc=-1; "
        "for f in /sys/class/thermal/thermal_zone*/temp; do "
        "  [ -r \\\"$f\\\" ] || continue; "
        "  v=$(cat \\\"$f\\\" 2>/dev/null); "
        "  case \\\"$v\\\" in ''|*[!0-9]*) continue ;; esac; "
        "  if [ $v -gt 1000 ]; then "
        "    c=$(awk 'BEGIN{printf \\\"%%.2f\\\", ("  // <-- %% fixed
        "($v/1000.0))}'); "
        "  else c=$v; fi; "
        "  if [ $v -gt $max ]; then max=$v; maxc=$c; fi; "
        "done; "
        "echo $maxc\"";

    FILE *fp = popen(cmd, "r");
    if (!fp) return -1.0;

    char buf[64] = {0};
    if (!fgets(buf, sizeof(buf), fp)) {
        pclose(fp);
        return -1.0;
    }
    pclose(fp);
    return atof(buf);
}

static double read_temp_c_remote(const char *user_at_host) {
    char cmd[2048];
    snprintf(cmd, sizeof(cmd),
        "ssh -o BatchMode=yes -o ConnectTimeout=6 %s "
        "\"sh -lc '"
        "max=-1; maxc=-1; "
        "for f in /sys/class/thermal/thermal_zone*/temp; do "
        "  [ -r \\\"$f\\\" ] || continue; "
        "  v=$(cat \\\"$f\\\" 2>/dev/null); "
        "  case \\\"$v\\\" in \\\"\\\"|*[!0-9]*) continue ;; esac; "
        "  if [ $v -gt 1000 ]; then "
        "    c=$(awk \\\"BEGIN{printf \\\\\\\"%%.2f\\\\\\\", (($v/1000.0))}\\\"); " // <-- %% fixed
        "  else c=$v; fi; "
        "  if [ $v -gt $max ]; then max=$v; maxc=$c; fi; "
        "done; "
        "echo $maxc'\"",
        user_at_host
    );

    FILE *fp = popen(cmd, "r");
    if (!fp) return -1.0;

    char buf[64] = {0};
    if (!fgets(buf, sizeof(buf), fp)) {
        pclose(fp);
        return -1.0;
    }
    pclose(fp);
    return atof(buf);
}

static int count_rendered_frames(const char *outdir) {
    char cmd[1024];
    snprintf(cmd, sizeof(cmd),
             "sh -lc \"ls %s/frame_*.png 2>/dev/null | wc -l\"",
             outdir);
    FILE *fp = popen(cmd, "r");
    if (!fp) return -1;
    char buf[64] = {0};
    if (!fgets(buf, sizeof(buf), fp)) {
        pclose(fp);
        return -1;
    }
    pclose(fp);
    return atoi(buf);
}

static int file_exists(const char *path) {
    return access(path, F_OK) == 0;
}

int main(void) {
    // =========================
    // CONFIG (your cluster)
    // =========================
    const char *HEAD_BLEND = "/srv/cluster-share/projects/classroom/classroom.blend";
    const char *WORK_BLEND = "/mnt/cluster-share/projects/classroom/classroom.blend";

    const char *HEAD_OUTDIR = "/srv/cluster-share/renders/classroom";
    const char *WORK_OUTDIR = "/mnt/cluster-share/renders/classroom";

    // Nodes (user@tailscale_ip)
    const char *GARGI   = "gargi@100.68.239.71";
    const char *AVINASH = "avinash@100.89.81.24";

    // Frame range (you already found this)
    const int START = 1;
    const int END   = 145;

    // Weighted split
    // Avinash: 1-70, Head: 71-110, Gargi: 111-145
    const int A_S=1,   A_E=70;
    const int H_S=71,  H_E=110;
    const int G_S=111, G_E=145;

    // Monitoring interval
    const int interval_sec = 15;

    // =========================
    // PRECHECKS
    // =========================
    if (!file_exists(HEAD_BLEND)) {
        fprintf(stderr, "[FATAL] Blend not found on HEAD: %s\n", HEAD_BLEND);
        fprintf(stderr, "Make sure project is in /srv/cluster-share/projects/classroom/\n");
        return 1;
    }

    // Ensure output dir exists on shared storage
    {
        char cmd[512];
        snprintf(cmd, sizeof(cmd), "mkdir -p %s", HEAD_OUTDIR);
        if (run_cmd(cmd) != 0) return 1;
    }

    // Ensure workers have mount + output dir (best-effort)
    run_cmd("ssh -o ConnectTimeout=6 gargi@100.68.239.71 \"mkdir -p /mnt/cluster-share/renders/classroom\"");
    run_cmd("ssh -o ConnectTimeout=6 avinash@100.89.81.24 \"mkdir -p /mnt/cluster-share/renders/classroom\"");

    // =========================
    // START RENDERS
    // =========================
    double t0 = now_sec();
    printf("Starting distributed render...\n");
    printf("Avinash: %d-%d | Head: %d-%d | Gargi: %d-%d\n", A_S, A_E, H_S, H_E, G_S, G_E);

    // Launch Gargi
    {
        char cmd[4096];
        snprintf(cmd, sizeof(cmd),
            "ssh -o ConnectTimeout=8 %s "
            "\"nohup blender -b %s -E CYCLES -s %d -e %d -a "
            "-o %s/frame_##### -F PNG > %s/gargi.log 2>&1 & echo GARGI_STARTED\"",
            GARGI, WORK_BLEND, G_S, G_E, WORK_OUTDIR, WORK_OUTDIR
        );
        run_cmd(cmd);
    }

    // Launch Avinash
    {
        char cmd[4096];
        snprintf(cmd, sizeof(cmd),
            "ssh -o ConnectTimeout=8 %s "
            "\"nohup blender -b %s -E CYCLES -s %d -e %d -a "
            "-o %s/frame_##### -F PNG > %s/avinash.log 2>&1 & echo AVINASH_STARTED\"",
            AVINASH, WORK_BLEND, A_S, A_E, WORK_OUTDIR, WORK_OUTDIR
        );
        run_cmd(cmd);
    }

    // Launch Head
    {
        char cmd[4096];
        snprintf(cmd, sizeof(cmd),
            "nohup blender -b %s -E CYCLES -s %d -e %d -a "
            "-o %s/frame_##### -F PNG > %s/head.log 2>&1 &",
            HEAD_BLEND, H_S, H_E, HEAD_OUTDIR, HEAD_OUTDIR
        );
        run_cmd(cmd);
        printf("HEAD_STARTED\n");
    }

    // =========================
    // METRICS (CSV)
    // =========================
    char metrics_path[512];
    snprintf(metrics_path, sizeof(metrics_path), "%s/cluster_metrics.csv", HEAD_OUTDIR);

    FILE *csv = fopen(metrics_path, "w");
    if (!csv) {
        perror("[FATAL] fopen metrics csv");
        return 1;
    }
    fprintf(csv, "elapsed_sec,frames_done,temp_head_c,temp_gargi_c,temp_avinash_c\n");
    fflush(csv);

    const int expected_total = END - START + 1;
    int last_done = -1;

    // =========================
    // MONITOR LOOP
    // =========================
    while (1) {
        int done = count_rendered_frames(HEAD_OUTDIR);

        double th = read_temp_c_local();
        double tg = read_temp_c_remote(GARGI);
        double ta = read_temp_c_remote(AVINASH);

        double elapsed = now_sec() - t0;

        fprintf(csv, "%.2f,%d,%.2f,%.2f,%.2f\n", elapsed, done, th, tg, ta);
        fflush(csv);

        if (done != last_done) {
            printf("Progress: %d/%d frames | Temps(C): head=%.2f gargi=%.2f avinash=%.2f\n",
                   done, expected_total, th, tg, ta);
            last_done = done;
        }

        if (done >= expected_total) break;
        sleep(interval_sec);
    }

    double total = now_sec() - t0;
    fclose(csv);

    printf("\nDONE ✅ Total wall time: %.2f sec (%.2f min)\n", total, total/60.0);
    printf("Metrics CSV: %s\n", metrics_path);
    printf("Logs: %s/head.log, %s/gargi.log, %s/avinash.log\n", HEAD_OUTDIR, HEAD_OUTDIR, HEAD_OUTDIR);

    return 0;
}
