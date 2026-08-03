CREATE TYPE cdc_color AS ENUM ('RED', 'GREEN', 'BLUE');

CREATE TABLE records
(
    id BIGINT NOT NULL PRIMARY KEY,
    sequence_no INTEGER NULL,
    boolean_value BOOLEAN NULL,
    byte_value SMALLINT NULL,
    small_value SMALLINT NULL,
    int_value INTEGER NULL,
    big_value BIGINT NULL,
    float_value REAL NULL,
    double_value DOUBLE PRECISION NULL,
    decimal_value DECIMAL(18, 4) NULL,
    char_value CHAR(4) NULL,
    varchar_value VARCHAR(64) NULL,
    text_value TEXT NULL,
    binary_value BYTEA NULL,
    date_value DATE NULL,
    time_value TIME(3) NULL,
    timestamp_value TIMESTAMP(6) NULL,
    unicode_value VARCHAR(64) NULL,
    nullable_value VARCHAR(64) NULL,
    uuid_value UUID NULL,
    json_value JSON NULL,
    jsonb_value JSONB NULL,
    enum_value cdc_color NULL
);

ALTER TABLE records REPLICA IDENTITY FULL;

INSERT INTO records
VALUES (1001,
        0,
        TRUE,
        -7,
        -1234,
        -123456789,
        -1234567890123456789,
        1.25,
        -12345.5,
        12345678901234.5678,
        'ABCD',
        'snapshot-varchar',
        'snapshot-text',
        decode('000102ff', 'hex'),
        DATE '2024-02-29',
        TIME '12:34:56.789',
        TIMESTAMP '2024-02-29 12:34:56.123456',
        '像素-快照🌟',
        NULL,
        '123e4567-e89b-12d3-a456-426614174000',
        '"json-snapshot"',
        '"jsonb-snapshot"',
        'GREEN');
