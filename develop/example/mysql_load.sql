LOAD
DATA INFILE '/var/lib/mysql-files/tpch_data/customer.tbl'
INTO TABLE customer
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

LOAD
DATA INFILE '/var/lib/mysql-files/tpch_data/lineitem.tbl'
INTO TABLE lineitem
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

LOAD
DATA INFILE '/var/lib/mysql-files/tpch_data/nation.tbl'
INTO TABLE nation
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

LOAD
DATA INFILE '/var/lib/mysql-files/tpch_data/orders.tbl'
INTO TABLE orders
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

LOAD
DATA INFILE '/var/lib/mysql-files/tpch_data/part.tbl'
INTO TABLE part
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

LOAD
DATA INFILE '/var/lib/mysql-files/tpch_data/partsupp.tbl'
INTO TABLE partsupp
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

LOAD
DATA INFILE '/var/lib/mysql-files/tpch_data/region.tbl'
INTO TABLE region
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

LOAD
DATA INFILE '/var/lib/mysql-files/tpch_data/supplier.tbl'
INTO TABLE supplier
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';