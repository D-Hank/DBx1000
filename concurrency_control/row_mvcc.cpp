//#include "mvcc.h"
#include "txn.h"
#include "row.h"
#include "manager.h"
#include "row_mvcc.h"
#include "mem_alloc.h"
#include <mm_malloc.h>

#if CC_ALG == MVCC

void Row_mvcc::init(row_t * row) {
	_row = row;
	_his_len = 4;
	_req_len = _his_len;

	_write_history = (WriteHisEntry *) _mm_malloc(sizeof(WriteHisEntry) * _his_len, 64);
	_requests = (ReqEntry *) _mm_malloc(sizeof(ReqEntry) * _req_len, 64);
	for (uint32_t i = 0; i < _his_len; i++) {
		_requests[i].valid = false;
		_write_history[i].valid = false;
		_write_history[i].reserved = false;
		_write_history[i].row = NULL;
	}
	_latest_row = _row;
	_latest_wts = 0;
	_oldest_wts = 0;

	_num_versions = 0;
	_exists_prewrite = false;
	_max_served_rts = 0;
	
	blatch = false;
	latch = (pthread_mutex_t *) _mm_malloc(sizeof(pthread_mutex_t), 64);
	pthread_mutex_init(latch, NULL);
}

void Row_mvcc::buffer_req(TsType type, txn_man * txn, bool served)
{
	uint32_t access_num = 1;
	while (true) {
		for (uint32_t i = 0; i < _req_len; i++) {
			// TODO No need to keep all read history.
			// in some sense, only need to keep the served read request with the max ts.
			// 
			if (!_requests[i].valid) { // Find an empty slot
				_requests[i].valid = true;
				_requests[i].type = type;
				_requests[i].ts = txn->get_ts();
				_requests[i].txn = txn;
				_requests[i].time = get_sys_clock();
				return;
			}
		}
		assert(access_num == 1); // Should succeed when we do one enlarging
		double_list(1); // Enlarge the req list until we can tolerate
		access_num ++;
	}
}


void 
Row_mvcc::double_list(uint32_t list)
{
	if (list == 0) {
		WriteHisEntry * temp = (WriteHisEntry *) _mm_malloc(sizeof(WriteHisEntry) * _his_len * 2, 64);
		for (uint32_t i = 0; i < _his_len; i++) {
			temp[i].valid = _write_history[i].valid;
			temp[i].reserved = _write_history[i].reserved;
			temp[i].ts = _write_history[i].ts;
			temp[i].row = _write_history[i].row;
		}
		for (uint32_t i = _his_len; i < _his_len * 2; i++) {
			temp[i].valid = false;
			temp[i].reserved = false;
			temp[i].row = NULL;
		}
		_mm_free(_write_history);
		_write_history = temp;
		_his_len = _his_len * 2;
	} else {
		assert(list == 1);
		ReqEntry * temp = (ReqEntry *) _mm_malloc(sizeof(ReqEntry) * _req_len * 2, 64);
		for (uint32_t i = 0; i < _req_len; i++) {
			temp[i].valid = _requests[i].valid;
			temp[i].type = _requests[i].type;
			temp[i].ts = _requests[i].ts;
			temp[i].txn = _requests[i].txn;
			temp[i].time = _requests[i].time;
		}
		for (uint32_t i = _req_len; i < _req_len * 2; i++) 
			temp[i].valid = false;
		_mm_free(_requests);
		_requests = temp;
		_req_len = _req_len * 2;
	}
}

RC Row_mvcc::access(txn_man * txn, TsType type, row_t * row) {
	RC rc = RCOK;
	ts_t ts = txn->get_ts(); // My start ts
uint64_t t1 = get_sys_clock();
	if (g_central_man)
		glob_manager->lock_row(_row);
	else // Default
		while (!ATOM_CAS(blatch, false, true))
			PAUSE // Add some delay
		//pthread_mutex_lock( latch );
uint64_t t2 = get_sys_clock();
INC_STATS(txn->get_thd_id(), debug4, t2 - t1);
// Critical section start
#if DEBUG_CC
	for (uint32_t i = 0; i < _req_len; i++)
		if (_requests[i].valid) {
			assert(_requests[i].ts > _latest_wts);
			if (_exists_prewrite)
				assert(_prewrite_ts < _requests[i].ts);
		}
#endif
	if (type == R_REQ) { // Reader's access try
		if (ts < _oldest_wts) // There's no version I can see: compare with the first version in chain
			// the version was already recycled... This should be very rare
			rc = Abort;
		else if (ts > _latest_wts) { // I'm one of the first to see this latest version
			if (_exists_prewrite && _prewrite_ts < ts) // But there's a prewriter before me
			{
				// exists a pending prewrite request before the current read. should wait.
				rc = WAIT;
				buffer_req(R_REQ, txn, false);
				txn->ts_ready = false;
			} else { 
				// should just read
				rc = RCOK;
				txn->cur_row = _latest_row;
				if (ts > _max_served_rts)
					_max_served_rts = ts;
			}
		} else { // There's someone wrote this record, read some past versions I can see
			rc = RCOK;
			// ts is between _oldest_wts and _latest_wts, should find the correct version
			uint32_t the_ts = 0; // Last ts I checked
		   	uint32_t the_i = _his_len;
	   		for (uint32_t i = 0; i < _his_len; i++) { // Find a version I can see
		   		if (_write_history[i].valid 
					&& _write_history[i].ts < ts 
			   		&& _write_history[i].ts > the_ts) 
	   			{
		   			the_ts = _write_history[i].ts;
			  		the_i = i; // Won't break, we will keep the newest among them (maybe just only one)
				}
			}
			if (the_i == _his_len) // There's no one in history I can see
				txn->cur_row = _row;
   			else 
	   			txn->cur_row = _write_history[the_i].row;
		}
	} else if (type == P_REQ) { // Writer's read phase
		if (ts < _latest_wts || ts < _max_served_rts || (_exists_prewrite && _prewrite_ts > ts))
			rc = Abort;
		else if (_exists_prewrite) {  // _prewrite_ts < ts (the above branch)
			rc = WAIT;
			// What if the same txn writes this row multiple times? Still wait?
			// What if txn A waits for this row held by txn B, while B waits for another row held by txn A?
			// Deadlock?
			buffer_req(P_REQ, txn, false);
			txn->ts_ready = false;
		} else {
			rc = RCOK;
			row_t * res_row = reserveRow(ts, txn);
			assert(res_row);
			res_row->copy(_latest_row); // Copy out original data
			txn->cur_row = res_row;
		}
	} else if (type == W_REQ) { // Writer's commit phase
		rc = RCOK; // Entering this means we must be the first committer
		assert(ts > _latest_wts);
		assert(row == _write_history[_prewrite_his_id].row);
		_write_history[_prewrite_his_id].valid = true;
		_write_history[_prewrite_his_id].ts = ts;
		_latest_wts = ts;
		_latest_row = row;
		_exists_prewrite = false;
		_num_versions ++;
		update_buffer(txn, W_REQ);
	} else if (type == XP_REQ) { // Aborted writer release his resources (reserved slots)
		assert(row == _write_history[_prewrite_his_id].row);
		_write_history[_prewrite_his_id].valid = false;
		_write_history[_prewrite_his_id].reserved = false;
		_exists_prewrite = false;
		update_buffer(txn, XP_REQ);
	} else 
		assert(false);
INC_STATS(txn->get_thd_id(), debug3, get_sys_clock() - t2);
	if (g_central_man)
		glob_manager->release_row(_row);
	else
		blatch = false; // Note that even if we release the lock, this version can belong to us
		//pthread_mutex_unlock( latch );	
// Critical section end	
	return rc;
}

row_t *
Row_mvcc::reserveRow(ts_t ts, txn_man * txn)
{
	assert(!_exists_prewrite); // We can only serve one prewriter
	
	// Garbage Collection
	ts_t min_ts = glob_manager->get_min_ts(txn->get_thd_id());
	if (_oldest_wts < min_ts && 
		_num_versions == _his_len) // Slots full and there's some (clearly) outdated versions
	{
		ts_t max_recycle_ts = 0; // Max ts of all recyclable versions
		ts_t idx = _his_len; // Index of last recyclable versions
		for (uint32_t i = 0; i < _his_len; i++) { // Maybe this loop will always succeed?
			if (_write_history[i].valid
				&& _write_history[i].ts < min_ts
				&& _write_history[i].ts > max_recycle_ts)		
			{
				max_recycle_ts = _write_history[i].ts;
				idx = i;
			}
		}
		// some entries can be garbage collected.
		if (idx != _his_len) {
			row_t * temp = _row;
			_row = _write_history[idx].row;
			_write_history[idx].row = temp; // Current version exchange with that slot
			_oldest_wts = max_recycle_ts;
			for (uint32_t i = 0; i < _his_len; i++) { // Clear those recyclable versions
				if (_write_history[i].valid
					&& _write_history[i].ts <= max_recycle_ts)
				{
					_write_history[i].valid = false;
					_write_history[i].reserved = false;
					assert(_write_history[i].row);
					_num_versions --;
				}
			}
		}
	}
	
#if DEBUG_CC
	uint32_t his_size = 0;
	uint64_t max_ts = 0;
	for (uint32_t i = 0; i < _his_len; i++) 
		if (_write_history[i].valid) {
			his_size ++;
			if (_write_history[i].ts > max_ts)
				max_ts = _write_history[i].ts;
		}
	assert(his_size == _num_versions);
	if (_num_versions > 0)
		assert(max_ts == _latest_wts);
#endif
	uint32_t idx = _his_len;
	// _write_history is not full, find an unused entry for P_REQ.
	if (_num_versions < _his_len) {
		for (uint32_t i = 0; i < _his_len; i++) {
			if (!_write_history[i].valid 
				&& !_write_history[i].reserved 
				&& _write_history[i].row != NULL) 
			{
				idx = i;
				break;
			}
			else if (!_write_history[i].valid 
				 	 && !_write_history[i].reserved)
				idx = i;
		}
		assert(idx < _his_len);
	}
	row_t * row;
	if (idx == _his_len) { // Not found, (maybe) used by currently running txn
		if (_his_len >= g_thread_cnt) {
			// all entries are taken. recycle the oldest version if _his_len is too long already
			ts_t min_ts = UINT64_MAX; 
			for (uint32_t i = 0; i < _his_len; i++) { // Find the one with minimum ts among all (valid) entries
				if (_write_history[i].valid && _write_history[i].ts < min_ts) {
					min_ts = _write_history[i].ts;
					idx = i;
				}
			}
			// Why? Try deleting? Since that estimated lower bound is not accurate?
			// One case is: txn with `min_ts` hasn't died and its version has not been recycled (_oldest_wts=min_ts) in the first loop
			// This case can be cleared and recycled here
			assert(min_ts > _oldest_wts);
			assert(_write_history[idx].row);
			// OK, suppose we can always find one slot, now make it empty
			// What if a txn writes a row multiple times?
			// Note that we only accept one prewriter
			row = _row;
			_row = _write_history[idx].row;
			_write_history[idx].row = row; // Exchange with the current active version
			_oldest_wts = min_ts;
			_num_versions --;
		} else {
			// double the history size. 
			double_list(0);
			_prewrite_ts = ts; // Not necessary since we will do this later
#if DEBUG_CC
			for (uint32_t i = 0; i < _his_len / 2; i++)
				assert(_write_history[i].valid);
			assert(!_write_history[_his_len / 2].valid);
#endif
			idx = _his_len / 2;
		}
	} 
	assert(idx != _his_len);
	// some entries are not taken. But the row of that entry is NULL.
	if (!_write_history[idx].row) { // Those initially empty ones contain an NULL row (and those allocated when enlarging)
		_write_history[idx].row = (row_t *) _mm_malloc(sizeof(row_t), 64);
		_write_history[idx].row->init(MAX_TUPLE_SIZE); // Alloc this when necessary
	}
	_write_history[idx].valid = false;
	_write_history[idx].reserved = true;
	_write_history[idx].ts = ts;
	_exists_prewrite = true;
	_prewrite_his_id = idx;
	_prewrite_ts = ts;
	return _write_history[idx].row;
}

void Row_mvcc::update_buffer(txn_man * txn, TsType type) {
	// the current txn performs WR or XP.
	// immediate following R_REQ and P_REQ should return.
	ts_t ts = txn->get_ts();
	// Find the earliest pending prewriters after me (writer)
	ts_t next_pre_ts = UINT64_MAX ;
	for (uint32_t i = 0; i < _req_len; i++)	
		if (_requests[i].valid && _requests[i].type == P_REQ
			&& _requests[i].ts > ts
			&& _requests[i].ts < next_pre_ts)
			next_pre_ts = _requests[i].ts;
	// return all pending quests between txn->ts and next_pre_ts
	for (uint32_t i = 0; i < _req_len; i++)	{
		if (_requests[i].valid)	
			assert(_requests[i].ts > ts);
		// return pending R_REQ 
		// Readers after me but before that prewriter are free to go (and let them read my output)
		if (_requests[i].valid && _requests[i].type == R_REQ && _requests[i].ts < next_pre_ts) {
			if (_requests[i].ts > _max_served_rts)
				_max_served_rts = _requests[i].ts;
			_requests[i].valid = false;
			_requests[i].txn->cur_row = _latest_row;
			_requests[i].txn->ts_ready = true;
		}
		// return one pending P_REQ
		// That earliest prewriter will be released from pending queue
		else if (_requests[i].valid && _requests[i].ts == next_pre_ts) {
			assert(_requests[i].type == P_REQ);
			row_t * res_row = reserveRow(_requests[i].ts, txn); // Passing my txn id here is ok
			assert(res_row);
			res_row->copy(_latest_row);
			_requests[i].valid = false;
			_requests[i].txn->cur_row = res_row;
			_requests[i].txn->ts_ready = true;
		}
	}
}

#endif
