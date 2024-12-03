#ifndef ROW_TS_H
#define ROW_TS_H

class table_t;
class Catalog;
class txn_man;
struct TsReqEntry {
	txn_man * txn;
	// for write requests, need to have a copy of the data to write.
	row_t * row;
	itemid_t * item;
	ts_t ts;
	TsReqEntry * next;
};
// There's some difference compared with Wu's survey.
// We only record a txn's start ts as ts here
class Row_ts {
public:
	void init(row_t * row);
	// Try to access a row, used both in read and commit phase
	RC access(txn_man * txn, TsType type, row_t * row);

private:
 	pthread_mutex_t * latch; // A mutex lock, for critical section
	bool blatch;
	// Put a txn into req buffer, wait for previous txn to commit (similar to reservation in MVCC)
	// It's more like reserve a slot, not only when someone is blocked do we do buffering
	void buffer_req(TsType type, txn_man * txn, row_t * row);
	// Debuffer a certain txn from the specified queue
	TsReqEntry * debuffer_req(TsType type, txn_man * txn);
	// Debuffer those txn with timestamp earlier than the given `ts`, return a list
	TsReqEntry * debuffer_req(TsType type, ts_t ts);
	TsReqEntry * debuffer_req(TsType type, txn_man * txn, ts_t ts);
	// Update all buffers, when some writer ends his job and and was taken out from the pre queue
	// Wake up order should be: I (writer) -> my P -> R -> W -> W's P
	void update_buffer();
	// Calculate min ts in a certain buffer
	ts_t cal_min(TsType type);
	// Alloc a req entry in local partition
	TsReqEntry * get_req_entry();
	// Release a certain req entry held by txn contained in `entry`
	void return_req_entry(TsReqEntry * entry);
	// Release those req entries held by used-to-be-pending requests in `list`
 	void return_req_list(TsReqEntry * list);

	row_t * _row; // Which row I manage (bidirectional)
	ts_t wts; // Last write ts of this row
    ts_t rts; // Last read ts of this row
    ts_t min_wts; // Minimum ts of all requests in `writereq` queue, it's up to date
    ts_t min_rts; // Minimum ts of all requests in `readreq` queue, it's up to date
    ts_t min_pts; // Minimum ts of all requests in `prereq` queue, it's up to date

    TsReqEntry * readreq; // All waiting txn holding an `R_REQ` tag, organized as a linked list
    TsReqEntry * writereq; // All waiting txn holding a `W_REQ` tag (writers blocked at their commit stage)
    TsReqEntry * prereq; // All waiting txn holding a `P_REQ` tag (writers blocked at their read phase)
	uint64_t preq_len; // Length of `prereq`, maybe not necessary
};

#endif
