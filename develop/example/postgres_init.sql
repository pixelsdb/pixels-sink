GRANT ALL PRIVILEGES ON DATABASE pixels_realtime_crud TO pixels;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO pixels;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO pixels;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO pixels;

\c pixels_realtime_crud;

-- \i /example/sql/dss.ddl;
