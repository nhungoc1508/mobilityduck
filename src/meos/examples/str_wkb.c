#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <postgres.h>
#include <utils/timestamp.h>
#include <meos.h>
#include <meos_internal.h>

#define MAX_INSTANTS    1000000
#define MAX_LENGTH_INST 64
#define WKB_EXTENDED 0x04

int main(void) {
	Temporal *instants[MAX_INSTANTS] = {0};

	meos_initialize();

    TimestampTz t = pg_timestamptz_in("2025-01-01 12:00:00", -1);
	printf("Number of instants to generate: %d\n", MAX_INSTANTS);
	for (int i = 0; i < MAX_INSTANTS; i++) {
        int value = i % 2 + 1;
        instants[i] = (Temporal *)tinstant_make(value, T_TINT, t);
    }

	clock_t time = clock();
    for (int i = 0; i < MAX_INSTANTS; i++) {
        char *str = temporal_out(instants[i], 15);
        Temporal *temp = temporal_in(str, T_TINT);
    }

	time = clock() - time;
	double time_taken = ((double)time) / CLOCKS_PER_SEC;
	printf("temporal_out() & temporal_in() took %f seconds\n", time_taken);

	time = clock();
	for (int i = 0; i < MAX_INSTANTS; i++) {
        size_t wkb_size;
        uint8_t *wkb = temporal_as_wkb(instants[i], WKB_EXTENDED, &wkb_size);
        Temporal *temp = temporal_from_wkb(wkb, wkb_size);
    }
    
	time = clock() - time;
	time_taken = ((double)time) / CLOCKS_PER_SEC;
	printf("temporal_as_wkb() & temporal_from_wkb() took %f seconds\n", time_taken);
	for (int i = 0; i < MAX_INSTANTS; i++)
		free(instants[i]);

	meos_finalize();
	return EXIT_SUCCESS;
}