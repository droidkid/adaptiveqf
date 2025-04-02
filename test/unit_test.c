#include <stdio.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include <openssl/rand.h>

#include "include/hashutil.h"
#include "include/rand_util.h"
#include "include/splinter_util.h"
#include "include/test_driver.h"


int test_merged_setup() { 
	size_t qbits = 8;
	size_t rbits = 8;

	size_t num_slots = 1ull << qbits;
	size_t minirun_id_bitmask = (1ull << (qbits + rbits)) - 1;

	data_config data_cfg = qf_data_config_init();
	splinterdb_config splinterdb_cfg = qf_splinterdb_config_init("db", &data_cfg);
	remove(splinterdb_cfg.filename);
	splinterdb *db;
	if (splinterdb_create(&splinterdb_cfg, &db)) {
    return -1;
	}
	splinterdb_lookup_result db_result;
	splinterdb_lookup_result_init(db, &db_result, 0, NULL);

	QF qf;
	if (!qf_malloc(&qf, num_slots, qbits + rbits, 0, QF_HASH_INVERTIBLE, 0)) {
    return -1;
	}

  int num_keys = 3;
  uint64_t keys[] = {0xC1010, 0xB1010, 0xA1010};
  for (int i=0; i < num_keys; i++) {
    fprintf(stderr, "Inserting key with hash %x\n", keys[i]);
	  int ret = qf_splinter_insert(&qf, db, keys[i], 1);
		if (ret == 1) continue;
		if (ret == 0) {
      fprintf(stderr, "Insertion failed at key %d: %x\n", i, keys[i]);
      return -1;
    };
  }

  char buffer[10 * MAX_VAL_SIZE];
  uint64_t hash;
  int minirun_rank;
  for (int i=0; i < num_keys; i++) {
	  if ((minirun_rank = qf_query_using_ll_table(&qf, keys[i], &hash, QF_KEY_IS_HASH)) >= 0) {
			hash = (hash & minirun_id_bitmask) << (64 - qf.metadata->quotient_remainder_bits);
      // DB Lookup.
			slice query = padded_slice(&hash, MAX_KEY_SIZE, sizeof(hash), buffer, 0);
			splinterdb_lookup(db, query, &db_result);
			slice result_val;
			splinterdb_lookup_result_value(&db_result, &result_val);
      int num_keys = result_val.length / MAX_VAL_SIZE;
      int found = 0;
      for (int j=0; j < num_keys; j++) {
			  if (memcmp(&keys[i], slice_data(result_val) + j * MAX_VAL_SIZE, sizeof(uint64_t)) == 0) {
          found = 1; break;
			  }
      }
      if (!found) {
        fprintf(stderr, " TEST FAILED: DB missing key %d: %x\n", i, keys[i]);
      }
    }
    else {
      fprintf(stderr, " TEST FAILED: Filter missing key %d: %x\n", i, keys[i]);
      return -1;
    }
  }

	splinterdb_close(&db);
	qf_free(&qf);
	return 0;
}

int test_split_setup() { 
  printf("---------- Merge setup test start -----------\n");
	size_t qbits = 8;
	size_t rbits = 8;

	size_t num_slots = 1ull << qbits;
	size_t minirun_id_bitmask = (1ull << (qbits + rbits)) - 1;

	data_config data_cfg = qf_data_config_init();
	splinterdb_config splinterdb_cfg = qf_splinterdb_config_init("db2", &data_cfg);
	remove(splinterdb_cfg.filename);
	splinterdb *db;
	if (splinterdb_create(&splinterdb_cfg, &db)) {
    return -1;
	}
	splinterdb_lookup_result db_result;
	splinterdb_lookup_result_init(db, &db_result, 0, NULL);

	data_config bm_data_cfg = qf_data_config_init();
	splinterdb_config backing_cfg = qf_splinterdb_config_init("bm2", &bm_data_cfg);
	remove(backing_cfg.filename);
	splinterdb *bm;
	if (splinterdb_create(&backing_cfg, &bm)) {
    return -1;
	}
	splinterdb_lookup_result bm_result;
	splinterdb_lookup_result_init(bm, &bm_result, 0, NULL);

	QF qf;
	if (!qf_malloc(&qf, num_slots, qbits + rbits, 0, QF_HASH_INVERTIBLE, 0)) {
    return -1;
	}

  int num_keys = 2;
  uint64_t keys[] = {0xC1010, 0xA1010};
  for (int i=0; i < num_keys; i++) {
    fprintf(stderr, "Inserting key with hash %x\n", keys[i]);
	  int ret = qf_splinter_insert_split(&qf, db, bm, keys[i], 1);
		if (ret == 1) continue;
		if (ret == 0) {
      fprintf(stderr, "Insertion failed at key %d: %x\n", i, keys[i]);
      return -1;
    };
  }

  int num_queries = 6;
  int expected_false_positives = 2; // Adapt once for key colliding key.
  uint64_t query_set[] = {0xD1010, 0xD1010, 0xD1010, 0xD1010, 0xD1010, 0xD1010};
  char buffer[10 * MAX_VAL_SIZE];
  uint64_t hash;
  int minirun_rank;
  int fp_count = 0;
  for (int i=0; i < num_queries; i++) {
	  if ((minirun_rank = qf_query_using_ll_table(&qf, query_set[i], &hash, QF_KEY_IS_HASH)) >= 0) {
			slice db_query = padded_slice(&query_set[i], MAX_KEY_SIZE, sizeof(query_set[i]), buffer, 0);
			splinterdb_lookup(db, db_query, &db_result);
			if (!splinterdb_lookup_found(&db_result)) {
        fp_count++;
				hash = (hash & minirun_id_bitmask) << (64 - qf.metadata->quotient_remainder_bits);
        // hash = (hash & minirun_id_bitmask);
				slice bm_query = padded_slice(&hash, MAX_KEY_SIZE, sizeof(hash), buffer, 0);
				splinterdb_lookup(bm, bm_query, &bm_result);
				slice result_val;
				splinterdb_lookup_result_value(&bm_result, &result_val);

        uint64_t num_results = result_val.length;
				uint64_t orig_key;
				memcpy(&orig_key, slice_data(result_val) + minirun_rank * MAX_VAL_SIZE, sizeof(uint64_t));
				qf_adapt_using_ll_table(&qf, orig_key, query_set[i], minirun_rank, QF_KEY_IS_HASH);
      }
    }
  }
  fprintf(stderr, "num false positives: %d expected fp:%d \n", fp_count, expected_false_positives);
	// splinterdb_close(&bm);
  splinterdb_close(&db);
	qf_free(&qf);
  printf("---------- Merge setup test done! -----------\n");
	return 0;
}

int main(int argc, char **argv)
{
  int test_to_run = atoi(argv[1]);
  // ./unit_test 1
  if (test_to_run == 1) {
    test_merged_setup();
  // ./unit_test 2
  } else if (test_to_run == 2) {
    test_split_setup();
  }
}
