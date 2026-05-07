#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

double get_time_sec() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

// very simple temp read (head only)
void print_local_temp() {
    FILE *fp = fopen("/sys/class/thermal/thermal_zone0/temp", "r");
    if (!fp) return;

    int temp;
    fscanf(fp, "%d", &temp);
    fclose(fp);

    printf("HEAD temp: %.2f C\n", temp / 1000.0);
}

int main() {

    printf("Starting cluster rendering...\n");

    double start = get_time_sec();

    // ---- START WORKERS ----
    system(
        "ssh gargi@100.68.239.71 "
        "\"nohup blender -b /mnt/cluster-share/projects/classroom/classroom.blend "
        "-E CYCLES -s 111 -e 145 -a "
        "-o /mnt/cluster-share/renders/classroom/frame_##### -F PNG "
        "> /mnt/cluster-share/renders/classroom/gargi.log 2>&1 &\""
    );

    system(
        "ssh avinash@100.89.81.24 "
        "\"nohup blender -b /mnt/cluster-share/projects/classroom/classroom.blend "
        "-E CYCLES -s 1 -e 70 -a "
        "-o /mnt/cluster-share/renders/classroom/frame_##### -F PNG "
        "> /mnt/cluster-share/renders/classroom/avinash.log 2>&1 &\""
    );

    // ---- HEAD RENDER ----
    system(
        "nohup blender -b /srv/cluster-share/projects/classroom/classroom.blend "
        "-E CYCLES -s 71 -e 110 -a "
        "-o /srv/cluster-share/renders/classroom/frame_##### -F PNG "
        "> /srv/cluster-share/renders/classroom/head.log 2>&1 &"
    );

    printf("All renders launched.\n");

    // ---- SIMPLE MONITOR LOOP ----
    for (int i = 0; i < 20; i++) {   // monitor for ~5 mins
        print_local_temp();
        sleep(15);
    }

    double end = get_time_sec();

    printf("\nRender launched successfully.\n");
    printf("Elapsed time (launcher runtime): %.2f sec\n", end - start);

    printf("\nCheck progress with:\n");
    printf("ls /srv/cluster-share/renders/classroom/frame_*.png | wc -l\n");

    return 0;
}
