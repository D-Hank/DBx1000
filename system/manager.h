#pragma once 

#include "helper.h"
#include "global.h"

class row_t;
class txn_man;

class Manager {
public:
	void 			init();
	// Get the next timestamp from (centralized) manager
	ts_t			get_ts(uint64_t thread_id);

	// Each thread will call this to register their `ts` in global manager
	// For MVCC. To calculate the min active ts in the system
	void 			add_ts(uint64_t thd_id, ts_t ts);
	// Returns (an estimation of) minimum (registered) ts of running txn
	ts_t 			get_min_ts(uint64_t tid = 0);

	// HACK! the following mutexes are used to model a centralized
	// lock/timestamp manager. 
 	void 			lock_row(row_t * row);
	void 			release_row(row_t * row);
	// Get my/others' registered txn
	txn_man * 		get_txn_man(int thd_id) { return _all_txns[thd_id]; };
	// A certain thread registers its txn in global structure
	void 			set_txn_man(txn_man * txn);
	
	uint64_t 		get_epoch() { return *_epoch; };
	void 	 		update_epoch();
private:
	// for SILO
	volatile uint64_t * _epoch;		
	ts_t * 			_last_epoch_update_time;

	pthread_mutex_t ts_mutex;
	uint64_t *		timestamp; // NOTE: Why alloc on heap instead of BSS?
	pthread_mutex_t mutexes[BUCKET_CNT]; // NOTE: Why fixed?
	uint64_t 		hash(row_t * row);
	// All timestamps registered my threads
	// NOTE: volatile means both the value and the pointers can change
	ts_t volatile * volatile * volatile all_ts;
	txn_man ** 		_all_txns;
	// for MVCC 
	volatile ts_t	_last_min_ts_time; // Last walltime when we maintain `_min_ts`. But why not updated? Always 0?
	ts_t			_min_ts; // Min ts of all running txn, only updated when necessary
};
