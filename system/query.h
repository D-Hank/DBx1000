#pragma once 

#include "global.h"
#include "helper.h"

class workload;
class ycsb_query;
class tpcc_query;

class base_query {
public:
	virtual void init(uint64_t thd_id, workload * h_wl) = 0;
	uint64_t waiting_time;
	uint64_t part_num; // How many partitions we will access
	uint64_t * part_to_access; // Partitions this query will (actually, means no repetition sampling) access
};

// All the queries (txns) for a particular thread.
class Query_thd {
public:
	// Compute queries (in this workload) assigned to this thread
	// Each thread will receive a fixed number of txn: `WARMUP / g_thread_cnt + MAX_TXN_PER_PART + 4`
	void init(workload * h_wl, int thread_id);
	base_query * get_next_query(); 
	int q_idx; // Which query this thread is processing
#if WORKLOAD == YCSB
	ycsb_query * queries; // All queries (txn) belongs to this thread
#else 
	tpcc_query * queries; // All queries (txn) belongs to this thread
#endif
	char pad[CL_SIZE - sizeof(void *) - sizeof(int)];
	drand48_data buffer;
};

// TODO we assume a separate task queue for each thread in order to avoid 
// contention in a centralized query queue. In reality, more sofisticated 
// queue model might be implemented.
class Query_queue {
public:
	// Each thread init its own queries and puts in its queue
	void init(workload * h_wl);
	void init_per_thread(int thread_id);
	base_query * get_next_query(uint64_t thd_id); 
	
private:
	static void * threadInitQuery(void * This);

	Query_thd ** all_queries;
	workload * _wl;
	static int _next_tid; // NOTE: Why BSS?
};
