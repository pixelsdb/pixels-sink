-- init.sql

GRANT ALL PRIVILEGES ON pixels_realtime_crud.* TO 'pixels'@'%';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'pixels'@'%';
GRANT SELECT, RELOAD, SHOW DATABASES ON *.* TO 'pixels'@'%';
GRANT ALL PRIVILEGES ON pixels_realtime_crud.* TO 'pixels'@'%';
GRANT FILE on *.* to pixels@'%';

FLUSH PRIVILEGES;

-- create & load tpch tables
USE pixels_realtime_crud;

-- SOURCE /var/lib/mysql-files/sql/dss.ddl;
