#pragma once

class table_t;
class Catalog;
class txn_man;

// Only a constant number of versions can be maintained.
// If a request accesses an old version that has been recycled,   
// simply abort the request.

#if CC_ALG == MVCC
struct WriteHisEntry {
	bool valid;		// whether the entry/slot contains a valid version
	bool reserved; 	// when valid == false, this field indicates whether the entry is reserved by a P_REQ. If both=false, this slot is not used
	ts_t ts;
	row_t * row;	// That old version/row
};

struct ReqEntry {
	bool valid;
	TsType type; // P_REQ or R_REQ
	ts_t ts;
	txn_man * txn;
	ts_t time;
};


class Row_mvcc {
public:
	void init(row_t * row);
	// Will get access to the required row itself
	RC access(txn_man * txn, TsType type, row_t * row);
private:
 	pthread_mutex_t * latch;
	volatile bool blatch; // A simple bool latch, used along with CAS

	row_t * _row;

	RC conflict(TsType type, ts_t ts, uint64_t thd_id = 0);
	// Update buffer when a writer is about to commit and finishes his job
	// Wake up those who wait for him
	// Maybe no need to pass this `type`?
	void update_buffer(txn_man * txn, TsType type);
	// Put a txn in pending request queue
	// `served` is not used
	void buffer_req(TsType type, txn_man * txn, bool served);

	// Invariant: all valid entries in _requests have greater ts than any entry in _write_history 
	row_t * 		_latest_row; // Latest version
	ts_t			_latest_wts; // Latest wts in the version chain (updated when write commit)
	ts_t			_oldest_wts; // Oldest wts in the version chain (updated when recycling)
	WriteHisEntry * _write_history; // Slots of write history
	// the following is a small optimization.
	// the timestamp for the served prewrite request.
	// There should be at most one served prewrite request.
	// prewrite means pending write (slots alloc, but not committed yet)
	bool  			_exists_prewrite;
	ts_t 			_prewrite_ts; // ts of that prewriter
	// Our reserved index in history buffer
	uint32_t 		_prewrite_his_id;
	ts_t 			_max_served_rts; // Max rts I served all the way

	// _requests only contains pending requests.
	ReqEntry * 		_requests; // Slots of pending requests
	uint32_t 		_his_len; // Number of write_history slots (capacity), we will usually fix it to 4
	uint32_t 		_req_len; // Number of requests slots (capacity)
	// Invariant: _num_versions <= 4
	// Invariant: _num_prewrite_reservation <= 2
	uint32_t 		_num_versions; // Number of (valid) versions we store in our history slots
	
	// list = 0: _write_history
	// list = 1: _requests
	void double_list(uint32_t list);
	// Reserve a slot/version in the write history for a prewriter
	// Will always succeed if we do enlarging and garbage collection
	// Passing other `txn` here is ok
	row_t * reserveRow(ts_t ts, txn_man * txn);
};

#endif
