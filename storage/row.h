#pragma once 

#include <cassert>
#include "global.h"

#define DECL_SET_VALUE(type) \
	void set_value(int col_id, type value);

#define SET_VALUE(type) \
	void row_t::set_value(int col_id, type value) { \
		set_value(col_id, &value); \
	}

#define DECL_GET_VALUE(type)\
	void get_value(int col_id, type & value);

#define GET_VALUE(type)\
	void row_t::get_value(int col_id, type & value) {\
		int pos = get_schema()->get_field_index(col_id);\
		value = *(type *)&data[pos];\
	}

class table_t;
class Catalog;
class txn_man;
class Row_lock;
class Row_mvcc;
class Row_hekaton;
class Row_ts;
class Row_occ;
class Row_tictoc;
class Row_silo;
class Row_vll;
// There could be some intermediate data structure for a row to help the manager?
// Alloc this meta data locally while the data itself using stdlib?
class row_t
{
public:

	RC init(table_t * host_table, uint64_t part_id, uint64_t row_id = 0);
	// Will simply alloc a buffer
	void init(int size);
	RC switch_schema(table_t * host_table);
	// not every row has a manager
	void init_manager(row_t * row);

	table_t * get_table();
	Catalog * get_schema();
	const char * get_table_name();
	uint64_t get_field_cnt();
	uint64_t get_tuple_size();
	uint64_t get_row_id() { return _row_id; };
	// Maybe there's no need to use a manager and points back here again?
	void copy(row_t * src);

	void 		set_primary_key(uint64_t key) { _primary_key = key; };
	uint64_t 	get_primary_key() {return _primary_key; };
	uint64_t 	get_part_id() { return _part_id; };

	void set_value(int id, void * ptr);
	void set_value(int id, void * ptr, int size);
	void set_value(const char * col_name, void * ptr);
	char * get_value(int id);
	char * get_value(char * col_name);
	
	DECL_SET_VALUE(uint64_t);
	DECL_SET_VALUE(int64_t);
	DECL_SET_VALUE(double);
	DECL_SET_VALUE(UInt32);
	DECL_SET_VALUE(SInt32);

	DECL_GET_VALUE(uint64_t);
	DECL_GET_VALUE(int64_t);
	DECL_GET_VALUE(double);
	DECL_GET_VALUE(UInt32);
	DECL_GET_VALUE(SInt32);


	void set_data(char * data, uint64_t size);
	char * get_data();

	void free_row();

	// for concurrency control. can be lock, timestamp etc.
	// Try to access `this` row with priority specified by `type` and transaction specified by `txn`.
	// Fill the content (can be a copy, or the row itself) you want in the reference `&row`
	// CC manager corresponding to our protocol will be called
	RC get_row(access_t type, txn_man * txn, row_t *& row);
	void return_row(access_t type, txn_man * txn, row_t * row);
	
  #if CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE
    Row_lock * manager;
  #elif CC_ALG == TIMESTAMP
   	Row_ts * manager;
  #elif CC_ALG == MVCC
  	Row_mvcc * manager;
  #elif CC_ALG == HEKATON
  	Row_hekaton * manager;
  #elif CC_ALG == OCC
  	Row_occ * manager; // This row's manager, allocated locally in partition
  #elif CC_ALG == TICTOC
  	Row_tictoc * manager;
  #elif CC_ALG == SILO
  	Row_silo * manager;
  #elif CC_ALG == VLL
  	Row_vll * manager;
  #endif
	char * data; // Real data of this row (stdlib?)
	table_t * table; // Belongs to which table
private:
	// primary key should be calculated from the data stored in the row.
	uint64_t 		_primary_key;
	uint64_t		_part_id; // In which partition
	uint64_t 		_row_id; // Which row I am in this table, never set and never used
};
