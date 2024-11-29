#ifndef ROW_OCC_H
#define ROW_OCC_H

class table_t;
class Catalog;
class txn_man;
struct TsReqEntry;
// A row/tuple/record's manager
class Row_occ {
public:
	void 				init(row_t * row);
	// Try to access (read) `this` row. If pass, (for OCC) a copy will be returned to manager `txn`
	RC 					access(txn_man * txn, TsType type);
	void 				latch();
	// ts is the start_ts of the validating txn, return `true` if not modified
	bool				validate(uint64_t ts);
	// Note that `this` is a row's manager, may control the original row (if it has)
	void				write(row_t * data, uint64_t ts);
	void 				release();
private:
 	pthread_mutex_t * 	_latch;
	bool 				blatch;

	row_t * 			_row; // The real content
	// the last update time
	ts_t 				wts;
};

#endif
