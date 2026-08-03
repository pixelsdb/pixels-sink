INSERT INTO records
VALUES (2002,
        1,
        FALSE,
        8,
        2345,
        123456789,
        1234567890123456789,
        2.5,
        67890.25,
        -12345678901234.5678,
        'WXYZ',
        'insert-varchar',
        'insert-text',
        decode('cafebabe', 'hex'),
        DATE '2025-01-02',
        TIME '01:02:03.004',
        TIMESTAMP '2025-01-02 03:04:05.654321',
        '像素-插入🚀',
        NULL,
        '123e4567-e89b-12d3-a456-426614174001',
        '"json-insert"',
        '"jsonb-insert"',
        'RED');

UPDATE records
SET sequence_no     = 2,
    boolean_value   = TRUE,
    byte_value      = -9,
    small_value     = -3456,
    int_value       = -987654321,
    big_value       = -2222222222222222222,
    float_value     = -3.75,
    double_value    = 0.125,
    decimal_value   = 99999999999999.9999,
    char_value      = 'IJKL',
    varchar_value   = 'update-varchar',
    text_value      = 'update-text',
    binary_value    = decode('ffeeddcc', 'hex'),
    date_value      = DATE '2026-06-07',
    time_value      = TIME '23:59:59.999',
    timestamp_value = TIMESTAMP '2026-06-07 08:09:10.111222',
    unicode_value   = '像素-更新🧪',
    nullable_value  = NULL,
    uuid_value      = '123e4567-e89b-12d3-a456-426614174002',
    json_value      = '"json-update"',
    jsonb_value     = '"jsonb-update"',
    enum_value      = 'BLUE'
WHERE id = 2002;

DELETE
FROM records
WHERE id = 2002;
