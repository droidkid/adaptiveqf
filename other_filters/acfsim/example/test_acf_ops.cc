#include "cuckoofilter.h"

#include <assert.h>
#include <math.h>
#include <time.h>

#include <iostream>
#include <vector>
#include <openssl/rand.h>

extern "C" {
#include <splinterdb/splinterdb.h>
#include <splinterdb/data.h>
#include <splinterdb/public_platform.h>
#include <splinterdb/default_data_config.h>
}

#define Kilo (1024UL)
#define Mega (1024UL * Kilo)
#define Giga (1024UL * Mega)

#define TEST_DB_NAME "db"

#define MAX_KEY_SIZE 16
#define MAX_VAL_SIZE 16

using cuckoofilter::CuckooFilter;

uint64_t rand_uniform(uint64_t max) {
        if (max <= RAND_MAX) return rand() % max;
        uint64_t a = rand();
        uint64_t b = rand();
        a |= (b << 31);
        return a % max;
}

double rand_zipfian(double s, double max, uint64_t source) {
        double p = (double)source / (-1ULL);

        double pD = p * (12 * (pow(max, -s + 1) - 1) / (1 - s) + 6 + 6 * pow(max, -s) + s - s * pow(max, -s + 1));
        double x = max / 2;
        while (true) {
                double m = pow(x, -s - 2);
                double mx = m * x;
                double mxx = mx * x;
                double mxxx = mxx * x;

                double b = 12 * (mxxx - 1) / (1 - s) + 6 + 6 * mxx + s - (s * mx) - pD;
                double c = 12 * mxx - (6 * s * mx) + (m * s * (s + 1));
                double newx = x - b / c > 1 ? x - b / c : 1;
                if (abs(newx - x) <= 0.01) { // this is the tolerance for approximation
                        return newx;
                }
                x = newx;
        }
}

void bp() {
	return;
}

void pad_data(void *dest, const void *src, const size_t dest_len, const size_t src_len, const int flagged) {
	assert(dest_len >= src_len);
	if (flagged) memset(dest, 0xf, dest_len);
	else bzero(dest, dest_len);
	memcpy(dest, src, src_len);
}

slice padded_slice(const void *data, const size_t dest_len, const size_t src_len, void *buffer, const int flagged) {
	pad_data(buffer, data, dest_len, src_len, flagged);

	return slice_create(dest_len, buffer);
}

int db_insert(splinterdb *database, const void *key_data, const size_t key_len, unsigned char *key_buffer, const void *val_data, const size_t val_len, unsigned char *val_buffer, const int flagged) {
	pad_data(key_buffer, key_data, MAX_KEY_SIZE, key_len, flagged);
	pad_data(val_buffer, val_data, MAX_VAL_SIZE, val_len, 0);
	slice key = slice_create(MAX_KEY_SIZE, key_buffer);
	slice val = slice_create(MAX_VAL_SIZE, val_buffer);

	return splinterdb_insert(database, key, val);
}

int main(int argc, char **argv) {
	uint64_t seed;
	size_t num_queries;
	size_t num_inc_queries;
	size_t nslots;
	if (argc < 4) {
		printf("Usage: ./test_ext_throughput [log of nslots] [num queries] [incr queries]\n");
		exit(1);
	}
	else if (argc >= 4) {
		nslots = 1ull << atoi(argv[1]);
		num_queries = strtoull(argv[2], NULL, 10);
		num_inc_queries = strtoull(argv[3], NULL, 10);
	}
	if (argc >= 5) {
		seed = strtoull(argv[4], NULL, 10);
		printf("Warning: seeding may not necessarily work due to openssl's own generator\n");
	}
	else {
		seed = time(NULL);
	}
	size_t num_inserts = nslots * 0.9;

	printf("running on seed %lu\n", seed);
	srand(seed);

	// Create a cuckoo filter where each item is of type size_t and
	// use 12 bits for each item:
	//    CuckooFilter<size_t, 12> filter(total_items);
	// To enable semi-sorting, define the storage of cuckoo filter to be
	// PackedTable, accepting keys of size_t type and making 13 bits
	// for each key:
	//   CuckooFilter<size_t, 13, cuckoofilter::PackedTable> filter(total_items);
	printf("initializing data structures...\n");
	CuckooFilter<size_t, 12> filter(num_inserts);

	data_config data_cfg;
        default_data_config_init(MAX_KEY_SIZE, &data_cfg);
        splinterdb_config splinterdb_cfg = (splinterdb_config){
                .filename   = "db",
                .cache_size = 64 * Mega,
                .disk_size  = 20 * Giga,
                .data_cfg   = &data_cfg
        };
        splinterdb *database;
        if (splinterdb_create(&splinterdb_cfg, &database) != 0) {
                printf("Error creating database\n");
                exit(0);
        }
        splinterdb_lookup_result db_result;
        splinterdb_lookup_result_init(database, &db_result, 0, NULL);

	printf("generating inserts...\n");
	uint64_t *inserts = new uint64_t[num_inserts];
	RAND_bytes((unsigned char*)(inserts), num_inserts * sizeof(uint64_t));

	printf("starting inserts...\n");

	unsigned char buffer[MAX_KEY_SIZE + MAX_VAL_SIZE];

	// Insert items to this cuckoo filter
	double measure_interval = 0.01f;
	double current_interval = measure_interval;
	uint64_t measure_point = nslots * current_interval, last_point = 0;

	FILE *inserts_fp = fopen("stats_splinter_inserts.csv", "w");
	fprintf(inserts_fp, "fill through\n");
	FILE *inc_queries_fp = fopen("stats_splinter_inc_queries.csv", "w");
	fprintf(inc_queries_fp, "fill through\n");

	clock_t start_time = clock(), end_time, interval_time = start_time;
	//clock_t query_start_time, query_end_time;

	size_t i;
	for (i = 0; i <= num_inserts; i++) {
		if (filter.AddUsingBackingMap(inserts[i], database, MAX_KEY_SIZE, MAX_VAL_SIZE, &db_result, buffer) != cuckoofilter::Ok) {
			printf("insert failed at fill rate %f\n", (double)i / nslots);
			break;
		}

		db_insert(database, &inserts[i], sizeof(inserts[i]), buffer, &i, sizeof(i), buffer + MAX_KEY_SIZE, 0);

		if (i >= measure_point) {
			fprintf(inserts_fp, "%f %f\n", current_interval * 100, (double)(i - last_point) * CLOCKS_PER_SEC / (clock() - interval_time));

			uint64_t *inc_queries = new uint64_t[num_inc_queries];
			RAND_bytes((unsigned char*)(inc_queries), num_inc_queries * sizeof(uint64_t));

			interval_time = clock();
			for (size_t j = 0; j < num_inc_queries; j++) {
				uint64_t location_data;
				if (filter.ContainReturn(inc_queries[j], &location_data) == cuckoofilter::Ok) {
					slice query_slice = padded_slice(&inc_queries[j], MAX_KEY_SIZE, sizeof(inc_queries[j]), buffer, 0);
					splinterdb_lookup(database, query_slice, &db_result);
				}
			}
			fprintf(inc_queries_fp, "%f %f\n", current_interval * 100, (double)(num_inc_queries) * CLOCKS_PER_SEC / (clock() - interval_time));

			fprintf(stderr, "\r%d%%", (int)(100 * current_interval));
			current_interval += measure_interval;
			last_point = measure_point;
			measure_point = nslots * current_interval;
		}
	}
	end_time = clock();

	fclose(inserts_fp);

	printf("\n");
	printf("made %lu inserts\n", i);
	printf("time per insert:      %f us\n", (double)(end_time - start_time) / i);
	printf("insert throughput:    %f ops/sec\n", (double)i * CLOCKS_PER_SEC / (end_time - start_time));
	
	/*uint64_t *query_set = (uint64_t*)calloc(total_items, sizeof(uint64_t));
	for (size_t i = 0; i < total_items; i++) {
		//query_set[i] = rand_zipfian(1.5f, 1lu << 30);
		query_set[i] = i + total_items;
	}*/

	// Check non-existing items, a few false positives expected
	size_t fp_queries = 0;
	//size_t p_queries = 0;
	/*for (size_t i = total_items; i < 2 * total_items; i++) {
	  if (filter.Contain(i) == cuckoofilter::Ok) {
	  false_queries++;
	  }
	  total_queries++;
	  }*/

	printf("generating queries...\n");
	uint64_t *queries = new uint64_t[num_queries];
	/*for (size_t i = 0; i < total_queries; i++) {
		queries[i] = rand_zipfian(1.5f, 1ull << 30);
		//queries[i] = rand_uniform();
	}*/
	RAND_bytes((unsigned char*)queries, num_queries * sizeof(uint64_t));
	for (i = 0; i < num_queries; i++) {
		queries[i] = (uint64_t)rand_zipfian(1.5f, 10000000ull, queries[i]);
	}

	printf("performing queries...\n");

	current_interval = measure_interval;
	measure_point = num_queries * current_interval;
	last_point = 0;
	FILE *queries_fp = fopen("stats_splinter_queries.csv", "w");
	fprintf(queries_fp, "queries through\n");
	FILE *fprates_fp = fopen("stats_splinter_fprates.csv", "w");
	fprintf(fprates_fp, "queries fprate\n");

	start_time = interval_time = clock();
	for (i = 0; i < num_queries; i++) {
		uint64_t location_data;
		if (filter.ContainReturn(queries[i], &location_data) == cuckoofilter::Ok) {
			slice query_slice = padded_slice(&queries[i], MAX_KEY_SIZE, sizeof(queries[i]), buffer, 0);
			splinterdb_lookup(database, query_slice, &db_result);

			if (!splinterdb_lookup_found(&db_result)) {
				fp_queries++;
				slice location_slice = padded_slice(&location_data, MAX_KEY_SIZE, sizeof(location_data), buffer, 1);
				splinterdb_lookup(database, location_slice, &db_result);

				slice result_val;
				splinterdb_lookup_result_value(&db_result, &result_val);
				uint64_t orig_key;
				memcpy(&orig_key, slice_data(result_val), sizeof(orig_key));

				//fprintf(stderr, "\rstarting adapt");
				if (filter.Adapt(orig_key, database, MAX_KEY_SIZE, MAX_VAL_SIZE, &db_result, buffer) != cuckoofilter::Ok) printf("error: adapt failed to find previously queried item\n");
				//fprintf(stderr, "\rfinished adapt");
			}
		}

		if (i >= measure_point) {
			fprintf(queries_fp, "%lu %f\n", i, (double)(i - last_point) * CLOCKS_PER_SEC / (clock() - interval_time));
			fprintf(fprates_fp, "%lu %f\n", i, (double)fp_queries / i);
			fprintf(stderr, "\r%d%%", (int)(current_interval * 100));
			current_interval += measure_interval;
			last_point = measure_point;
			measure_point = num_queries * current_interval;
			interval_time = clock();
		}
	}
	end_time = clock();

	fclose(queries_fp);
	fclose(fprates_fp);

	printf("made %lu queries\n", num_queries);
	printf("time per query:       %f us\n", (double)(end_time - start_time) / num_queries);
	printf("query throughput:     %f ops/sec\n", (double)num_queries * CLOCKS_PER_SEC / (end_time - start_time));

	printf("false positive rate:  %f%%\n", 100. * fp_queries / num_queries);

	return 0;
}
