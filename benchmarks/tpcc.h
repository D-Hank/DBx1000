#ifndef _TPCC_H_
#define _TPCC_H_

#include "wl.h"
#include "txn.h"

class table_t;
class INDEX;
class tpcc_query;
// All threads share this same workload
class tpcc_wl : public workload {
public:
	RC init();
	RC init_table();
	RC init_schema(const char * schema_file);
	RC get_txn_man(txn_man *& txn_manager, thread_t * h_thd);
	table_t * 		t_warehouse; // The WAREHOUSE table
	table_t * 		t_district; // The DISTRICT table
	table_t * 		t_customer; // The CUSTOMER table
	table_t *		t_history; // The HISTORY table
	table_t *		t_neworder; // The NEW-ORDER table
	table_t *		t_order; // The ORDER table
	table_t *		t_orderline; // The ORDERLINE table
	table_t *		t_item; // The ITEM table
	table_t *		t_stock; // The STOCK table

	INDEX * 	i_item; // The ITEM_IDX index (primary key)
	INDEX * 	i_warehouse; // The WAREHOUSE_IDX index (primary key)
	INDEX * 	i_district; // The DISTRICT_IDX index (primary key)
	INDEX * 	i_customer_id; // The CUSTOMER_IDX index (primary key)
	INDEX * 	i_customer_last; // The CUSTOMER_LAST_IDX index (primary key)
	INDEX * 	i_stock; // The STOCK_IDX index (primary key)
	INDEX * 	i_order; // The ORDER_IDX index (foreign key), key = (w_id, d_id, o_id)
	INDEX * 	i_orderline; // The ORDERLINE_IDX index (foreign key), key = (w_id, d_id, o_id)
	INDEX * 	i_orderline_wd; // The ORDERLINE_WD_IDX index (foreign key), key = (w_id, d_id). 
	
	bool ** delivering;
	uint32_t next_tid;
private:
	uint64_t num_wh; // Number of warehouses
	void init_tab_item();
	void init_tab_wh(uint32_t wid);
	void init_tab_dist(uint64_t w_id);
	void init_tab_stock(uint64_t w_id);
	void init_tab_cust(uint64_t d_id, uint64_t w_id);
	void init_tab_hist(uint64_t c_id, uint64_t d_id, uint64_t w_id);
	void init_tab_order(uint64_t d_id, uint64_t w_id);
	
	void init_permutation(uint64_t * perm_c_id, uint64_t wid);

	static void * threadInitItem(void * This);
	static void * threadInitWh(void * This);
	static void * threadInitDist(void * This);
	static void * threadInitStock(void * This);
	static void * threadInitCust(void * This);
	static void * threadInitHist(void * This);
	static void * threadInitOrder(void * This);

	static void * threadInitWarehouse(void * This);
};

class tpcc_txn_man : public txn_man
{
public:
	void init(thread_t * h_thd, workload * h_wl, uint64_t part_id); 
	RC run_txn(base_query * query);
private:
	tpcc_wl * _wl;
	RC run_payment(tpcc_query * m_query);
	RC run_new_order(tpcc_query * m_query);
	RC run_order_status(tpcc_query * query);
	RC run_delivery(tpcc_query * query);
	RC run_stock_level(tpcc_query * query);
};

#endif
