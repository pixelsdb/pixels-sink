-- init.sql

GRANT ALL PRIVILEGES ON pixels_realtime_crud.* TO 'pixels'@'%';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'pixels'@'%';
GRANT SELECT, RELOAD, SHOW DATABASES ON *.* TO 'pixels'@'%';
GRANT ALL PRIVILEGES ON pixels_realtime_crud.* TO 'pixels'@'%';
GRANT FILE on *.* to pixels@'%';

FLUSH PRIVILEGES;

-- create & load tpch tables
USE pixels_realtime_crud;

SOURCE /var/lib/mysql-files/sql/dss.ddl;

LOAD DATA INFILE '/var/lib/mysql-files/tpch_data/customer.tbl'
INTO TABLE customer
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

LOAD DATA INFILE '/var/lib/mysql-files/tpch_data/lineitem.tbl'
INTO TABLE lineitem
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

LOAD DATA INFILE '/var/lib/mysql-files/tpch_data/nation.tbl'
INTO TABLE nation
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

LOAD DATA INFILE '/var/lib/mysql-files/tpch_data/orders.tbl'
INTO TABLE orders
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

LOAD DATA INFILE '/var/lib/mysql-files/tpch_data/part.tbl'
INTO TABLE part
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

LOAD DATA INFILE '/var/lib/mysql-files/tpch_data/partsupp.tbl'
INTO TABLE partsupp
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

LOAD DATA INFILE '/var/lib/mysql-files/tpch_data/region.tbl'
INTO TABLE region
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

LOAD DATA INFILE '/var/lib/mysql-files/tpch_data/supplier.tbl'
INTO TABLE supplier
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

