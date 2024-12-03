#include "txn.h"
#include "row.h"
#include "row_ts.h"
#include "mem_alloc.h"
#include "manager.h"
#include "stdint.h"

void Row_ts::init(row_t * row) {
	_row = row;
	uint64_t part_id = row->get_part_id();
	wts = 0;
	rts = 0;
	min_wts = UINT64_MAX;
    min_rts = UINT64_MAX;
    min_pts = UINT64_MAX;
	readreq = NULL;
    writereq = NULL;
    prereq = NULL;
	preq_len = 0;
	latch = (pthread_mutex_t *) 
		mem_allocator.alloc(sizeof(pthread_mutex_t), part_id);
	pthread_mutex_init( latch, NULL );
	blatch = false;
}

TsReqEntry * Row_ts::get_req_entry() {
	uint64_t part_id = get_part_id(_row);
	return (TsReqEntry *) mem_allocator.alloc(sizeof(TsReqEntry), part_id);
}

void Row_ts::return_req_entry(TsReqEntry * entry) {
	if (entry->row != NULL) {
		entry->row->free_row();
		mem_allocator.free(entry->row, sizeof(row_t));
	}
	mem_allocator.free(entry, sizeof(TsReqEntry));
}

void Row_ts::return_req_list(TsReqEntry * list) {	
	TsReqEntry * req = list;
	TsReqEntry * prev = NULL;
	while (req != NULL) {
		prev = req;
		req = req->next;
		return_req_entry(prev);
	}
}

void Row_ts::buffer_req(TsType type, txn_man * txn, row_t * row)
{
	TsReqEntry * req_entry = get_req_entry();
	assert(req_entry != NULL);
	req_entry->txn = txn;
	req_entry->row = row;
	req_entry->ts = txn->get_ts();
	if (type == R_REQ) {
		req_entry->next = readreq;
		readreq = req_entry;
		if (req_entry->ts < min_rts)
			min_rts = req_entry->ts;
	} else if (type == W_REQ) {
		assert(row != NULL);
		req_entry->next = writereq;
		writereq = req_entry;
		if (req_entry->ts < min_wts)
			min_wts = req_entry->ts;
	} else if (type == P_REQ) {
		preq_len ++;
		req_entry->next = prereq;
		prereq = req_entry;
		if (req_entry->ts < min_pts)
			min_pts = req_entry->ts;
	}
}

TsReqEntry * Row_ts::debuffer_req(TsType type, txn_man * txn) {
	return debuffer_req(type, txn, UINT64_MAX);
}
	
TsReqEntry * Row_ts::debuffer_req(TsType type, ts_t ts) {
	return debuffer_req(type, NULL, ts);
}

TsReqEntry * Row_ts::debuffer_req( TsType type, txn_man * txn, ts_t ts ) {
	TsReqEntry ** queue;
	TsReqEntry * return_queue = NULL; // Node (if multiple, will be head of that list) to return
	switch (type) {
		case R_REQ : queue = &readreq; break;
		case P_REQ : queue = &prereq; break;
		case W_REQ : queue = &writereq; break;
		default: assert(false);
	}

	TsReqEntry * req = *queue; // Currently checking node
	TsReqEntry * prev_req = NULL; // Previous node
	if (txn != NULL) {
		while (req != NULL && req->txn != txn) {		
			prev_req = req;
			req = req->next;
		}
		assert(req != NULL);
		if (prev_req != NULL)
			prev_req->next = req->next;
		else {
			assert( req == *queue );
			*queue = req->next;
		}
		preq_len --;
		req->next = return_queue;
		return_queue = req;
	} else {
		while (req != NULL) { // Search to the end
			if (req->ts <= ts) {
				if (prev_req == NULL) { // The first node
					assert(req == *queue);
					*queue = (*queue)->next;
				} else { // In the middle
					prev_req->next = req->next;
				}
				req->next = return_queue;
				return_queue = req; // Take this node out
				req = (prev_req == NULL)? *queue : prev_req->next;
			} else {
				prev_req = req;
				req = req->next;
			}
		}
	}
	return return_queue;
}

ts_t Row_ts::cal_min(TsType type) {
	// update the min_pts
	TsReqEntry * queue;
	switch (type) {
		case R_REQ : queue = readreq; break;
		case P_REQ : queue = prereq; break;
		case W_REQ : queue = writereq; break;
		default: assert(false);
	}
	ts_t new_min_pts = UINT64_MAX;
	TsReqEntry * req = queue;
	while (req != NULL) {
		if (req->ts < new_min_pts)
			new_min_pts = req->ts;
		req = req->next;
	}
	return new_min_pts;
}

RC Row_ts::access(txn_man * txn, TsType type, row_t * row) {
	RC rc = RCOK;
	ts_t ts = txn->get_ts();
	if (g_central_man)
		glob_manager->lock_row(_row);
	else
		pthread_mutex_lock( latch ); // Use mutex instead of CAS (Sec 4.3)
	if (type == R_REQ) { // When a reader calls me for the first time
		if (ts < wts) { // Already wrote by someone else, first writer wins (Sec 2.2)
			rc = Abort;
		} else if (ts > min_pts) { // Someone comes before me is already waiting for writers to commit, I should certainly wait
			// insert the req into the read request queue
			buffer_req(R_REQ, txn, NULL);
			txn->ts_ready = false; // I will be waken up by this
			rc = WAIT;
		} else { // Good, I'm one of the first to access this row
			// return the value (a copy for read).
			txn->cur_row->copy(_row);
			if (rts < ts)
				rts = ts;
			rc = RCOK;
		}
	} else if (type == P_REQ) { // When a writer calls me for the first time
		if (ts < rts) { // Someone already reads this record before me (Sec 2.2)
			rc = Abort;
		} else {
#if TS_TWR
			buffer_req(P_REQ, txn, NULL);
			rc = RCOK;
#else 
			if (ts < wts) { // Sec 2.2
				rc = Abort;
			} else { // One of the first to access this row, wait to commit
				buffer_req(P_REQ, txn, NULL); // Will not set `ts_ready` since there's no wait like R_REQ and we will deal with its commit phase very soon
				rc = RCOK;
			}
#endif
		}
	} else if (type == W_REQ) { // A second call, my (writer's) commit stage
		// write requests are always accepted.
		rc = RCOK;
#if TS_TWR
		// according to TWR, this write is already stale, ignore. 
		if (ts < wts) {
			TsReqEntry * req = debuffer_req(P_REQ, txn);
			assert(req != NULL);
			update_buffer();
			return_req_entry(req);
			row->free_row();
			mem_allocator.free(row, sizeof(row_t));
			goto final;
		}
#else
		if (ts > min_pts) {
			buffer_req(W_REQ, txn, row);
			goto final;
		}
#endif
		if (ts > min_rts) {
			buffer_req(W_REQ, txn, row);
            goto final;
		} else { 
			// the write is output. 
			// Finally let me write (entering the commit phase), and will be read by those wait for me
			_row->copy(row);
			if (wts < ts)
				wts = ts;
			// debuffer the P_REQ
			// Note that this `P` entry is related to the `W` tag of this branch, so we need to take it out
			// Which means I (writer) is in the commit phase (will succeeed) and is taking out my read phase entry
			TsReqEntry * req = debuffer_req(P_REQ, txn); // I was previously a pending req in preq queue, take me out
			assert(req != NULL);
			update_buffer();
			return_req_entry(req);
			// the "row" is freed after hard copy to "_row"
			row->free_row();
			mem_allocator.free(row, sizeof(row_t));
		}
	} else if (type == XP_REQ) { // Write abort before committing, clear my occupied `P` slot
		TsReqEntry * req = debuffer_req(P_REQ, txn);
		assert (req != NULL);
		update_buffer();
		return_req_entry(req);
	} else 
		assert(false);
	
final:
	if (g_central_man)
		glob_manager->release_row(_row);
	else
		pthread_mutex_unlock( latch );
	return rc;
}

void Row_ts::update_buffer() {
	while (true) {
		// A writer's entry was just taken out from `P` queue, `min_pts` (must) increase
		ts_t new_min_pts = cal_min(P_REQ);
		// There should be some change when the writer (for whom everyone is waiting) finishes
		assert(new_min_pts >= min_pts);
		if (new_min_pts > min_pts)
			min_pts = new_min_pts; // The next pending request
		else break; // min_pts is not updated.
		// NOTE: debuffering order: in `access`, readers (r) are blocked by `p` tag and (to-commit-)writers (w) are blocked by `p` and `r` tag
		// Wake up readers blocked by prewriters. ready_read can be a list
		TsReqEntry * ready_read = debuffer_req(R_REQ, min_pts);
		if (ready_read == NULL) break;
		// for each debuffered readreq, perform read.
		TsReqEntry * req = ready_read;
		while (req != NULL) {			// These readers are ready now, read what the writer just wrote
			req->txn->cur_row->copy(_row);
			if (rts < req->ts)
				rts = req->ts;
			req->txn->ts_ready = true;
			req = req->next; // Note that we haven't release the lock, these are read-only
		}
		// return all the req_entry back to freelist
		return_req_list(ready_read);
		// re-calculate min_rts
		ts_t new_min_rts = cal_min(R_REQ);
		if (new_min_rts > min_rts)
			min_rts = new_min_rts;
		else break; // If no readers are awakened
		// Wake up writers blocked by the above readers
		TsReqEntry * ready_write = debuffer_req(W_REQ, min_rts);
		if (ready_write == NULL) break;
		ts_t young_ts = UINT64_MAX; // The earliest ts among these writers
		TsReqEntry * young_req = NULL; // The earliest writer
		req = ready_write;
		while (req != NULL) {
			TsReqEntry * tmp_req = debuffer_req(P_REQ, req->txn); // Also debuffer the corresponding P entry
			assert(tmp_req != NULL);
			return_req_entry(tmp_req);
			if (req->ts < young_ts) { // What does it mean by young?
				young_ts = req->ts;
				young_req = req;
			} //else loser = req;
			req = req->next;
		}
		// perform write.
		// Why let first writer wins?
		// Or we follow the timestamp ordering?
		// But it seems that we haven't abort those who arrive late
		_row->copy(young_req->row);
		if (wts < young_req->ts)
			wts = young_req->ts;
		return_req_list(ready_write);
	}
}

