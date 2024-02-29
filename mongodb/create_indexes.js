db.supplier.createIndex({ "S_NATIONKEY": 1 });

db.partsupp.createIndex({ "PS_PARTKEY": 1 });
db.partsupp.createIndex({ "PS_SUPPKEY": 1 });

db.customer.createIndex({ "C_NATIONKEY": 1 });

db.orders.createIndex({ "O_CUSTKEY": 1 });

db.lineitem.createIndex({ "L_ORDERKEY": 1 });
db.lineitem.createIndex({ "L_PARTKEY": 1, "L_SUPPKEY": 1 });

db.nation.createIndex({ "N_REGIONKEY": 1 });

db.lineitem.createIndex({ "L_SHIPDATE": 1, "L_DISCOUNT": 1, "L_QUANTITY": 1 });

db.orders.createIndex({ "O_ORDERDATE": 1 });
