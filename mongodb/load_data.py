import pandas as pd
import numpy as np
from pymongo import MongoClient

# init MongoClient
client = MongoClient()

# new database
db = client.tpch

# part
part = db.part
part_data = pd.read_csv('/home/dbs/tpch/mongodb/data/part.tbl', sep='|', header=None)
part_data = part_data.drop(9, axis=1)
part_data.columns = ['_id', 'P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE', 'P_COMMENT']
dict_part_data = [x[1].to_dict() for x in part_data.iterrows()]
part.insert_many(dict_part_data)
part_data = None
print(part.count_documents({}))

# supplier
sup = db.supplier
sup_data = pd.read_csv('/home/dbs/tpch/mongodb/data/supplier.tbl', sep='|', header=None)
sup_data = sup_data.drop(7, axis=1)
sup_data.columns = ['_id', 'S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT']
dict_sup_data = [x[1].to_dict() for x in sup_data.iterrows()]
sup.insert_many(dict_sup_data)
sup_data = None
print(sup.count_documents({}))

# partsupp
psup = db.partsupp
psup_data = pd.read_csv('/home/dbs/tpch/mongodb/data/partsupp.tbl', sep='|', header=None)
psup_data = psup_data.drop(5, axis=1)
psup_data.columns = ['PS_PARTKEY', 'PS_SUPPKEY', 'PS_AVAILQTY', 'PS_SUPPLYCOST', 'PS_COMMENT']
dict_psup_data = [x[1].to_dict() for x in psup_data.iterrows()]
psup.insert_many(dict_psup_data)
psup_data = None
print(psup.count_documents({}))

# customer
customer = db.customer
cust_data = pd.read_csv('/home/dbs/tpch/mongodb/data/customer.tbl', sep='|', header=None)
cust_data = cust_data.drop(8, axis=1)
cust_data.columns = ['_id', 'C_NAME', 'C_ADDRESS', 'C_NATIONKEY', 'C_PHONE', 'C_ACCTBAL', 'MKT_SEGMENT', 'C_COMMENT']
dict_cust_data = [x[1].to_dict() for x in cust_data.iterrows()]
customer.insert_many(dict_cust_data)
cust_data = None
print(customer.count_documents({}))

# orders
orders = db.orders
orders_data = pd.read_csv('/home/dbs/tpch/mongodb/data/orders.tbl', sep='|', header=None)
orders_data = orders_data.drop(9, axis=1)
orders_data.columns = ['_id', 'O_CUSTKEY', 'O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE', 'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT']
dict_orders_data = [x[1].to_dict() for x in orders_data.iterrows()]
orders.insert_many(dict_orders_data)
orders_data = None
print(orders.count_documents({}))

# lineitem
lineit = db.lineitem
lineit_data = pd.read_csv('/home/dbs/tpch/mongodb/data/lineitem.tbl', sep='|', header=None)
lineit_data = lineit_data.drop(16, axis=1)
lineit_data.columns = ['L_ORDERKEY', 'L_PARTKEY', 'L_SUPPKEY', 'L_LINENUMBER', 'L_QUANTITY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_TAX',
                       'L_RETURNFLAG', 'L_LINESTATUS', 'L_SHIPDATE', 'L_COMMITDATE', 'L_RECEIPTDATE', 'L_SHIPINSTRUCT', 'L_SHIPMODE', 'L_COMMENT']
dict_lineit_data = [x[1].to_dict() for x in lineit_data.iterrows()]
lineit.insert_many(dict_lineit_data)
lineit_data = None
print(lineit.count_documents({}))

# nation
nat = db.nation
nat_data = pd.read_csv('/home/dbs/tpch/mongodb/data/nation.tbl', sep='|', header=None)
nat_data = nat_data.drop(4, axis=1)
nat_data.columns = ['_id', 'N_NAME', 'N_REGIONKEY', 'N_COMMENT']
dict_nat_data = [x[1].to_dict() for x in nat_data.iterrows()]
nat.insert_many(dict_nat_data)
nat_data = None
print(nat.count_documents({}))

# region
reg = db.region
reg_data = pd.read_csv('/home/dbs/tpch/mongodb/data/region.tbl', sep='|', header=None)
reg_data = reg_data.drop(3, axis=1)
reg_data.columns = ['_id', 'R_NAME', 'R_COMMENT']
dict_reg_data = [x[1].to_dict() for x in reg_data.iterrows()]
reg.insert_many(dict_reg_data)
reg_data = None
print(reg.count_documents({}))