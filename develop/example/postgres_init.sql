GRANT ALL PRIVILEGES ON DATABASE pixels_realtime_crud TO pixels;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO pixels;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO pixels;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO pixels;

\c pixels_realtime_crud;

\i /example/sql/dss.ddl;

\COPY customer FROM '/example/tpch_data/customer.tbl' WITH (FORMAT csv, DELIMITER '|');

\COPY lineitem FROM '/example/tpch_data/lineitem.tbl' WITH (FORMAT csv, DELIMITER '|');

\COPY nation FROM '/example/tpch_data/nation.tbl' WITH (FORMAT csv, DELIMITER '|');

\COPY orders FROM '/example/tpch_data/orders.tbl' WITH (FORMAT csv, DELIMITER '|');

\COPY part FROM '/example/tpch_data/part.tbl' WITH (FORMAT csv, DELIMITER '|');

\COPY partsupp FROM '/example/tpch_data/partsupp.tbl' WITH (FORMAT csv, DELIMITER '|');

\COPY region FROM '/example/tpch_data/region.tbl' WITH (FORMAT csv, DELIMITER '|');

\COPY supplier FROM '/example/tpch_data/supplier.tbl' WITH (FORMAT csv, DELIMITER '|');