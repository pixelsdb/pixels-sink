/*
 * Copyright 2025 PixelsDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pixelsdb.pixels.sink.conversion.debezium.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.sink.SinkProto;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class DebeziumRowValueConverter
{
    private static final String CONNECT_DATE = "org.apache.kafka.connect.data.Date";
    private static final String CONNECT_TIME = "org.apache.kafka.connect.data.Time";
    private static final String CONNECT_TIMESTAMP = "org.apache.kafka.connect.data.Timestamp";
    private static final String CONNECT_DECIMAL = "org.apache.kafka.connect.data.Decimal";
    private static final String DEBEZIUM_DATE = "io.debezium.time.Date";
    private static final String DEBEZIUM_TIME = "io.debezium.time.Time";
    private static final String DEBEZIUM_MICRO_TIME = "io.debezium.time.MicroTime";
    private static final String DEBEZIUM_NANO_TIME = "io.debezium.time.NanoTime";
    private static final String DEBEZIUM_TIMESTAMP = "io.debezium.time.Timestamp";
    private static final String DEBEZIUM_MICRO_TIMESTAMP = "io.debezium.time.MicroTimestamp";
    private static final String DEBEZIUM_NANO_TIMESTAMP = "io.debezium.time.NanoTimestamp";
    private static final long MILLIS_PER_DAY = 86_400_000L;

    private final TypeDescription schema;

    public DebeziumRowValueConverter(TypeDescription schema)
    {
        this.schema = schema;
    }

    private static void buildFloat32(float value, SinkProto.ColumnValue.Builder columnValueBuilder)
    {
        buildInt32(Float.floatToIntBits(value), 4, columnValueBuilder);
    }

    private static void buildInt32(int value, int capacity, SinkProto.ColumnValue.Builder columnValueBuilder)
    {
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        switch (capacity)
        {
            case Byte.BYTES -> buffer.put((byte) value);
            case Integer.BYTES -> buffer.putInt(value);
            default -> throw new IllegalArgumentException("Unsupported integer width: " + capacity);
        }
        byte[] bytes = buffer.array();
        columnValueBuilder.setValue(ByteString.copyFrom(bytes));
    }

    public void parse(GenericRecord record, SinkProto.RowValue.Builder builder)
    {
        for (int i = 0; i < schema.getFieldNames().size(); i++)
        {
            String fieldName = schema.getFieldNames().get(i);
            TypeDescription fieldType = schema.getChildren().get(i);
            builder.addValues(parseCanonicalValue(record.get(fieldName), fieldName, fieldType).build());
        }
    }

    public void parse(JsonNode node, SinkProto.RowValue.Builder builder)
    {
        for (int i = 0; i < schema.getFieldNames().size(); i++)
        {
            String fieldName = schema.getFieldNames().get(i);
            TypeDescription fieldType = schema.getChildren().get(i);
            builder.addValues(parseValue(node.get(fieldName), fieldName, fieldType).build());
        }
    }

    public void parse(Struct record, SinkProto.RowValue.Builder builder)
    {
        for (int i = 0; i < schema.getFieldNames().size(); i++)
        {
            String fieldName = schema.getFieldNames().get(i);
            TypeDescription fieldType = schema.getChildren().get(i);
            Field field = record.schema().field(fieldName);
            if (field == null)
            {
                throw new IllegalArgumentException("Missing field in Debezium row schema: " + fieldName);
            }
            builder.addValues(parseConnectValue(
                    record.get(field), fieldName, field.schema(), fieldType).build());
        }
    }

    private SinkProto.ColumnValue.Builder parseConnectValue(
            Object raw, String fieldName, Schema connectSchema, TypeDescription type)
    {
        String schemaName = connectSchema.name();
        if (isUnsupportedLogicalType(schemaName))
        {
            throw unsupported(fieldName, "logical type " + schemaName);
        }
        if (connectSchema.type() == Schema.Type.ARRAY
                || connectSchema.type() == Schema.Type.MAP
                || connectSchema.type() == Schema.Type.STRUCT)
        {
            throw unsupported(fieldName, "Connect " + connectSchema.type());
        }
        if (type.getCategory() == TypeDescription.Category.STRUCT
                || type.getCategory() == TypeDescription.Category.VECTOR)
        {
            throw unsupported(fieldName, "Pixels " + type.getCategory());
        }
        if (raw == null)
        {
            return SinkProto.ColumnValue.newBuilder().setValue(ByteString.EMPTY);
        }

        if (schemaName != null)
        {
            switch (schemaName)
            {
                case CONNECT_DATE:
                    requireTarget(fieldName, type, TypeDescription.Category.DATE, schemaName);
                    requireSchemaType(fieldName, connectSchema, Schema.Type.INT32);
                    if (!(raw instanceof java.util.Date date))
                    {
                        throw invalidValue(fieldName, schemaName, raw);
                    }
                    return encodeInt32(
                            org.apache.kafka.connect.data.Date.fromLogical(connectSchema, date));
                case DEBEZIUM_DATE:
                    requireTarget(fieldName, type, TypeDescription.Category.DATE, schemaName);
                    requireSchemaType(fieldName, connectSchema, Schema.Type.INT32);
                    return encodeInt32(toIntExact(raw, fieldName));
                case CONNECT_TIME:
                    requireTarget(fieldName, type, TypeDescription.Category.TIME, schemaName);
                    requireSchemaType(fieldName, connectSchema, Schema.Type.INT32);
                    if (!(raw instanceof java.util.Date time))
                    {
                        throw invalidValue(fieldName, schemaName, raw);
                    }
                    return encodeTime(
                            org.apache.kafka.connect.data.Time.fromLogical(connectSchema, time),
                            1L, fieldName, type);
                case DEBEZIUM_TIME:
                    requireTarget(fieldName, type, TypeDescription.Category.TIME, schemaName);
                    requireSchemaType(fieldName, connectSchema, Schema.Type.INT32);
                    return encodeTime(toLongExact(raw, fieldName), 1L, fieldName, type);
                case DEBEZIUM_MICRO_TIME:
                    requireTarget(fieldName, type, TypeDescription.Category.TIME, schemaName);
                    requireSchemaType(fieldName, connectSchema, Schema.Type.INT64);
                    return encodeTime(toLongExact(raw, fieldName), 1_000L, fieldName, type);
                case DEBEZIUM_NANO_TIME:
                    requireTarget(fieldName, type, TypeDescription.Category.TIME, schemaName);
                    requireSchemaType(fieldName, connectSchema, Schema.Type.INT64);
                    return encodeTime(toLongExact(raw, fieldName), 1_000_000L, fieldName, type);
                case CONNECT_TIMESTAMP:
                    requireTarget(fieldName, type, TypeDescription.Category.TIMESTAMP, schemaName);
                    requireSchemaType(fieldName, connectSchema, Schema.Type.INT64);
                    if (!(raw instanceof java.util.Date timestamp))
                    {
                        throw invalidValue(fieldName, schemaName, raw);
                    }
                    return encodeTimestamp(
                            org.apache.kafka.connect.data.Timestamp.fromLogical(
                                    connectSchema, timestamp),
                            1_000L, fieldName, type);
                case DEBEZIUM_TIMESTAMP:
                    requireTarget(fieldName, type, TypeDescription.Category.TIMESTAMP, schemaName);
                    requireSchemaType(fieldName, connectSchema, Schema.Type.INT64);
                    return encodeTimestamp(
                            toLongExact(raw, fieldName), 1_000L, fieldName, type);
                case DEBEZIUM_MICRO_TIMESTAMP:
                    requireTarget(fieldName, type, TypeDescription.Category.TIMESTAMP, schemaName);
                    requireSchemaType(fieldName, connectSchema, Schema.Type.INT64);
                    return encodeTimestamp(
                            toLongExact(raw, fieldName), 1L, fieldName, type);
                case DEBEZIUM_NANO_TIMESTAMP:
                    requireTarget(fieldName, type, TypeDescription.Category.TIMESTAMP, schemaName);
                    requireSchemaType(fieldName, connectSchema, Schema.Type.INT64);
                    return encodeTimestampFromNanos(
                            toLongExact(raw, fieldName), fieldName, type);
                case CONNECT_DECIMAL:
                    return encodeConnectDecimal(raw, fieldName, connectSchema, type);
                default:
                    break;
            }
        }

        return switch (type.getCategory())
        {
            case BOOLEAN -> encodeBoolean(raw, fieldName, connectSchema);
            case BYTE, SHORT, INT, LONG -> encodeSignedInteger(
                    toConnectInteger(raw, fieldName, connectSchema), fieldName, type);
            case FLOAT -> encodeFloat(raw, fieldName, connectSchema);
            case DOUBLE -> encodeDouble(raw, fieldName, connectSchema);
            case CHAR, VARCHAR, STRING -> encodeString(raw, fieldName, connectSchema);
            case BINARY, VARBINARY -> encodeBinary(raw, fieldName, connectSchema);
            case DECIMAL -> throw unsupported(
                    fieldName, "DECIMAL without the fixed-scale " + CONNECT_DECIMAL + " schema");
            case DATE, TIME, TIMESTAMP -> throw unsupported(
                    fieldName, type.getCategory() + " without a supported logical schema name");
            case STRUCT, VECTOR -> throw unsupported(
                    fieldName, "Pixels " + type.getCategory());
        };
    }

    private SinkProto.ColumnValue.Builder encodeConnectDecimal(
            Object raw, String fieldName, Schema connectSchema, TypeDescription type)
    {
        requireSchemaType(fieldName, connectSchema, Schema.Type.BYTES);
        if (!(raw instanceof BigDecimal decimal))
        {
            throw invalidValue(fieldName, CONNECT_DECIMAL, raw);
        }
        String scaleParameter = connectSchema.parameters() == null
                ? null : connectSchema.parameters().get("scale");
        if (scaleParameter == null)
        {
            throw unsupported(fieldName, CONNECT_DECIMAL + " without a fixed scale");
        }

        final int sourceScale;
        try
        {
            sourceScale = Integer.parseInt(scaleParameter);
        } catch (NumberFormatException e)
        {
            throw new IllegalArgumentException(
                    "Invalid decimal scale for field '" + fieldName + "': " + scaleParameter, e);
        }

        if (type.getCategory() == TypeDescription.Category.DECIMAL)
        {
            return encodeDecimal(decimal, fieldName, type);
        }

        if (isSignedInteger(type.getCategory()) && sourceScale == 0)
        {
            final BigInteger integer;
            try
            {
                integer = decimal.toBigIntegerExact();
            } catch (ArithmeticException e)
            {
                throw new IllegalArgumentException(
                        "Decimal field '" + fieldName + "' is not an exact integer", e);
            }
            return encodeSignedInteger(integer, fieldName, type);
        }
        throw unsupported(fieldName, CONNECT_DECIMAL + " to Pixels " + type.getCategory());
    }

    /**
     * Encodes a decimal into the canonical Pixels byte format, i.e. the big-endian unscaled
     * value at the target scale: 8 bytes for short decimals and 16 bytes for long decimals.
     */
    private static SinkProto.ColumnValue.Builder encodeDecimal(
            BigDecimal value, String fieldName, TypeDescription type)
    {
        final BigDecimal normalized;
        try
        {
            normalized = value.setScale(type.getScale(), RoundingMode.UNNECESSARY);
        } catch (ArithmeticException e)
        {
            throw new IllegalArgumentException(
                    "Decimal field '" + fieldName + "' cannot be represented at scale "
                            + type.getScale() + " without rounding", e);
        }
        if (normalized.precision() > type.getPrecision())
        {
            throw new IllegalArgumentException(
                    "Decimal field '" + fieldName + "' exceeds precision "
                            + type.getPrecision() + ": " + normalized.toPlainString());
        }
        BigInteger unscaled = normalized.unscaledValue();
        if (type.getPrecision() <= TypeDescription.MAX_SHORT_DECIMAL_PRECISION)
        {
            return encodeInt64(unscaled.longValueExact());
        }
        ByteBuffer buffer = ByteBuffer.allocate(2 * Long.BYTES);
        buffer.putLong(unscaled.shiftRight(Long.SIZE).longValue());
        buffer.putLong(unscaled.longValue());
        return SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFrom(buffer.array()));
    }

    private SinkProto.ColumnValue.Builder encodeBoolean(
            Object raw, String fieldName, Schema connectSchema)
    {
        final boolean value;
        if (connectSchema.type() == Schema.Type.BOOLEAN && raw instanceof Boolean booleanValue)
        {
            value = booleanValue;
        }
        else if (isConnectInteger(connectSchema.type()))
        {
            BigInteger integer = toIntegralValue(raw, fieldName);
            if (!integer.equals(BigInteger.ZERO) && !integer.equals(BigInteger.ONE))
            {
                throw new IllegalArgumentException(
                        "Boolean field '" + fieldName + "' must be encoded as 0 or 1, got "
                                + integer);
            }
            value = integer.equals(BigInteger.ONE);
        }
        else
        {
            throw invalidValue(fieldName, "BOOLEAN or integral 0/1", raw);
        }
        return SinkProto.ColumnValue.newBuilder().setValue(
                ByteString.copyFrom(new byte[]{(byte) (value ? 1 : 0)}));
    }

    private SinkProto.ColumnValue.Builder encodeSignedInteger(
            BigInteger value, String fieldName, TypeDescription type)
    {
        BigInteger min;
        BigInteger max;
        int valueBits;
        int wireWidth;
        switch (type.getCategory())
        {
            case BYTE:
                min = BigInteger.valueOf(Byte.MIN_VALUE);
                max = BigInteger.valueOf(Byte.MAX_VALUE);
                valueBits = Byte.SIZE;
                wireWidth = Byte.BYTES;
                break;
            case SHORT:
                min = BigInteger.valueOf(Short.MIN_VALUE);
                max = BigInteger.valueOf(Short.MAX_VALUE);
                valueBits = Short.SIZE;
                // Pixels encodes SHORT with the same width as INT.
                wireWidth = Integer.BYTES;
                break;
            case INT:
                min = BigInteger.valueOf(Integer.MIN_VALUE);
                max = BigInteger.valueOf(Integer.MAX_VALUE);
                valueBits = Integer.SIZE;
                wireWidth = Integer.BYTES;
                break;
            case LONG:
                min = BigInteger.valueOf(Long.MIN_VALUE);
                max = BigInteger.valueOf(Long.MAX_VALUE);
                valueBits = Long.SIZE;
                wireWidth = Long.BYTES;
                break;
            default:
                throw unsupported(fieldName, "integer to Pixels " + type.getCategory());
        }
        if (value.compareTo(min) < 0 || value.compareTo(max) > 0)
        {
            throw new IllegalArgumentException(
                    "Integer field '" + fieldName + "' is outside the signed "
                            + valueBits + "-bit range: " + value);
        }
        if (wireWidth == Long.BYTES)
        {
            return encodeInt64(value.longValue());
        }
        SinkProto.ColumnValue.Builder builder = SinkProto.ColumnValue.newBuilder();
        buildInt32(value.intValue(), wireWidth, builder);
        return builder;
    }

    private SinkProto.ColumnValue.Builder encodeFloat(
            Object raw, String fieldName, Schema connectSchema)
    {
        requireSchemaType(fieldName, connectSchema, Schema.Type.FLOAT32);
        if (!(raw instanceof Float value))
        {
            throw invalidValue(fieldName, "FLOAT32", raw);
        }
        SinkProto.ColumnValue.Builder builder = SinkProto.ColumnValue.newBuilder();
        buildFloat32(value, builder);
        return builder;
    }

    private SinkProto.ColumnValue.Builder encodeDouble(
            Object raw, String fieldName, Schema connectSchema)
    {
        requireSchemaType(fieldName, connectSchema, Schema.Type.FLOAT64);
        if (!(raw instanceof Double value))
        {
            throw invalidValue(fieldName, "FLOAT64", raw);
        }
        return encodeInt64(Double.doubleToLongBits(value));
    }

    private SinkProto.ColumnValue.Builder encodeString(
            Object raw, String fieldName, Schema connectSchema)
    {
        requireSchemaType(fieldName, connectSchema, Schema.Type.STRING);
        if (!(raw instanceof String value))
        {
            throw invalidValue(fieldName, "STRING", raw);
        }
        return SinkProto.ColumnValue.newBuilder().setValue(
                ByteString.copyFrom(value, StandardCharsets.UTF_8));
    }

    private SinkProto.ColumnValue.Builder encodeBinary(
            Object raw, String fieldName, Schema connectSchema)
    {
        requireSchemaType(fieldName, connectSchema, Schema.Type.BYTES);
        if (raw instanceof byte[] bytes)
        {
            return SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFrom(bytes));
        }
        if (raw instanceof ByteBuffer buffer)
        {
            return SinkProto.ColumnValue.newBuilder().setValue(
                    ByteString.copyFrom(toBytes(buffer)));
        }
        throw invalidValue(fieldName, "BYTES", raw);
    }

    private SinkProto.ColumnValue.Builder encodeTime(
            long rawValue, long sourceUnitsPerMilli, String fieldName, TypeDescription type)
    {
        if (rawValue % sourceUnitsPerMilli != 0)
        {
            throw new IllegalArgumentException(
                    "Time field '" + fieldName + "' has sub-millisecond precision");
        }
        long millis = rawValue / sourceUnitsPerMilli;
        if (millis < 0 || millis >= MILLIS_PER_DAY)
        {
            throw new IllegalArgumentException(
                    "Time field '" + fieldName + "' is outside one day: " + millis + " ms");
        }
        requirePrecision(fieldName, millis, type.getPrecision(), 3);
        return encodeInt32(Math.toIntExact(millis));
    }

    private SinkProto.ColumnValue.Builder encodeTimestamp(
            long rawValue, long microsPerSourceUnit, String fieldName, TypeDescription type)
    {
        final long micros;
        try
        {
            micros = Math.multiplyExact(rawValue, microsPerSourceUnit);
        } catch (ArithmeticException e)
        {
            throw new IllegalArgumentException(
                    "Timestamp field '" + fieldName + "' overflows microseconds", e);
        }
        requirePrecision(fieldName, micros, type.getPrecision(), 6);
        return encodeInt64(micros);
    }

    private SinkProto.ColumnValue.Builder encodeTimestampFromNanos(
            long nanos, String fieldName, TypeDescription type)
    {
        if (nanos % 1_000L != 0)
        {
            throw new IllegalArgumentException(
                    "Timestamp field '" + fieldName + "' has sub-microsecond precision");
        }
        return encodeTimestamp(nanos / 1_000L, 1L, fieldName, type);
    }

    private static void requirePrecision(
            String fieldName, long value, int targetPrecision, int storagePrecision)
    {
        long factor = 1L;
        for (int i = targetPrecision; i < storagePrecision; i++)
        {
            factor *= 10L;
        }
        if (value % factor != 0)
        {
            throw new IllegalArgumentException(
                    "Field '" + fieldName + "' cannot be represented at precision "
                            + targetPrecision + " without truncation");
        }
    }

    private static SinkProto.ColumnValue.Builder encodeInt32(int value)
    {
        SinkProto.ColumnValue.Builder builder = SinkProto.ColumnValue.newBuilder();
        buildInt32(value, Integer.BYTES, builder);
        return builder;
    }

    private static SinkProto.ColumnValue.Builder encodeInt64(long value)
    {
        return SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFrom(
                ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
    }

    private static BigInteger toConnectInteger(
            Object raw, String fieldName, Schema connectSchema)
    {
        if (!isConnectInteger(connectSchema.type()))
        {
            throw unsupported(fieldName, "Connect " + connectSchema.type() + " to integer");
        }
        return toIntegralValue(raw, fieldName);
    }

    private static BigInteger toIntegralValue(Object raw, String fieldName)
    {
        if (raw instanceof Byte || raw instanceof Short
                || raw instanceof Integer || raw instanceof Long)
        {
            return BigInteger.valueOf(((Number) raw).longValue());
        }
        if (raw instanceof BigInteger integer)
        {
            return integer;
        }
        if (raw instanceof BigDecimal decimal)
        {
            try
            {
                return decimal.toBigIntegerExact();
            } catch (ArithmeticException e)
            {
                throw new IllegalArgumentException(
                        "Integer field '" + fieldName + "' has a fractional value", e);
            }
        }
        throw invalidValue(fieldName, "integral value", raw);
    }

    private static int toIntExact(Object raw, String fieldName)
    {
        try
        {
            return toIntegralValue(raw, fieldName).intValueExact();
        } catch (ArithmeticException e)
        {
            throw new IllegalArgumentException(
                    "Field '" + fieldName + "' is outside the signed 32-bit range", e);
        }
    }

    private static long toLongExact(Object raw, String fieldName)
    {
        try
        {
            return toIntegralValue(raw, fieldName).longValueExact();
        } catch (ArithmeticException e)
        {
            throw new IllegalArgumentException(
                    "Field '" + fieldName + "' is outside the signed 64-bit range", e);
        }
    }

    private static boolean isConnectInteger(Schema.Type type)
    {
        return type == Schema.Type.INT8 || type == Schema.Type.INT16
                || type == Schema.Type.INT32 || type == Schema.Type.INT64;
    }

    private static boolean isSignedInteger(TypeDescription.Category category)
    {
        return category == TypeDescription.Category.BYTE
                || category == TypeDescription.Category.SHORT
                || category == TypeDescription.Category.INT
                || category == TypeDescription.Category.LONG;
    }

    private static boolean isUnsupportedLogicalType(String schemaName)
    {
        return "io.debezium.data.VariableScaleDecimal".equals(schemaName)
                || "io.debezium.time.ZonedTime".equals(schemaName)
                || "io.debezium.time.ZonedTimestamp".equals(schemaName)
                || "io.debezium.time.Interval".equals(schemaName)
                || "io.debezium.time.MicroDuration".equals(schemaName)
                || schemaName != null && schemaName.startsWith("io.debezium.data.geometry.");
    }

    private static void requireTarget(
            String fieldName, TypeDescription type, TypeDescription.Category expected,
            String logicalType)
    {
        if (type.getCategory() != expected)
        {
            throw unsupported(
                    fieldName, logicalType + " to Pixels " + type.getCategory());
        }
    }

    private static void requireSchemaType(
            String fieldName, Schema schema, Schema.Type expected)
    {
        if (schema.type() != expected)
        {
            throw new IllegalArgumentException(
                    "Field '" + fieldName + "' schema " + schema.name() + " must use "
                            + expected + ", got " + schema.type());
        }
    }

    private static IllegalArgumentException invalidValue(
            String fieldName, String expected, Object raw)
    {
        return new IllegalArgumentException(
                "Field '" + fieldName + "' requires " + expected + ", got "
                        + raw.getClass().getName());
    }

    private static UnsupportedOperationException unsupported(
            String fieldName, String mapping)
    {
        return new UnsupportedOperationException(
                "Unsupported lossless mapping for field '" + fieldName + "': " + mapping);
    }

    private SinkProto.ColumnValue.Builder parseValue(JsonNode valueNode, String fieldName, TypeDescription type)
    {
        if (valueNode == null || valueNode.isNull())
        {
            return SinkProto.ColumnValue.newBuilder()
                    // .setName(fieldName)
                    .setValue(ByteString.EMPTY);
        }

        SinkProto.ColumnValue.Builder columnValueBuilder = SinkProto.ColumnValue.newBuilder();

        switch (type.getCategory())
        {
            case BYTE:
            {
                buildInt32(valueNode.asInt(), Byte.BYTES, columnValueBuilder);
                break;
            }
            case SHORT:
            {
                buildInt32(valueNode.asInt(), Integer.BYTES, columnValueBuilder);
                break;
            }
            case INT:
            {
                buildInt32(valueNode.asInt(), Integer.BYTES, columnValueBuilder);
                // columnValueBuilder.setType(PixelsProto.Type.newBuilder().setKind(PixelsProto.Type.Kind.INT));
                break;
            }
            case LONG:
            {
                long value = valueNode.asLong();
                byte[] bytes = ByteBuffer.allocate(Long.BYTES).putLong(value).array();
                columnValueBuilder.setValue(ByteString.copyFrom(bytes));
                // columnValueBuilder.setType(PixelsProto.Type.newBuilder().setKind(PixelsProto.Type.Kind.LONG));
                break;
            }
            case CHAR:
            {
                String text = valueNode.asText();
                byte[] bytes = new byte[]{(byte) text.charAt(0)};
                columnValueBuilder.setValue(ByteString.copyFrom(bytes));
                // columnValueBuilder.setType(PixelsProto.Type.newBuilder()
//                        .setKind(PixelsProto.Type.Kind.STRING));
                break;
            }
            case VARCHAR:
            case STRING:
            case VARBINARY:
            {
                String value = valueNode.asText();
                columnValueBuilder.setValue(ByteString.copyFrom(value, StandardCharsets.UTF_8));
                // columnValueBuilder.setType(PixelsProto.Type.newBuilder().setKind(PixelsProto.Type.Kind.STRING));
                break;
            }
            case DECIMAL:
            {
                return encodeDecimal(parseDecimal(valueNode, type), fieldName, type);
            }
            case BINARY:
            {
                String base64 = valueNode.asText(); // assume already base64 encoded
                columnValueBuilder.setValue(ByteString.copyFrom(base64, StandardCharsets.UTF_8));
                // columnValueBuilder.setType(PixelsProto.Type.newBuilder().setKind(PixelsProto.Type.Kind.BINARY));
                break;
            }
            case STRUCT:
            {
                // You can recursively parse fields in a struct here
                throw new UnsupportedOperationException("STRUCT parsing not yet implemented");
            }
            case DOUBLE:
            {
                double value = valueNode.asDouble();
                long longBits = Double.doubleToLongBits(value);
                byte[] bytes = ByteBuffer.allocate(Long.BYTES).putLong(longBits).array();
                columnValueBuilder.setValue(ByteString.copyFrom(bytes));
                // columnValueBuilder.setType(PixelsProto.Type.newBuilder().setKind(PixelsProto.Type.Kind.DOUBLE));
                break;
            }
            case FLOAT:
            {
                float value = (float) valueNode.asDouble();
                buildFloat32(value, columnValueBuilder);
                // columnValueBuilder.setType(PixelsProto.Type.newBuilder().setKind(PixelsProto.Type.Kind.FLOAT));
                break;
            }
            case DATE:
            case TIME:
            {
                buildInt32(valueNode.asInt(), Integer.BYTES, columnValueBuilder);
                // columnValueBuilder.setType(PixelsProto.Type.newBuilder()
                //        .setKind(PixelsProto.Type.Kind.DATE));
                break;
            }
            case TIMESTAMP:
            {
                long timestamp = valueNode.asLong();
                byte[] bytes = ByteBuffer.allocate(Long.BYTES).putLong(timestamp).array();
                columnValueBuilder.setValue(ByteString.copyFrom(bytes));
                // columnValueBuilder.setType(PixelsProto.Type.newBuilder()
                //        .setKind(PixelsProto.Type.Kind.DATE));
                break;
            }
            default:
                throw new IllegalArgumentException("Unsupported type: " + type.getCategory());
        }

        return columnValueBuilder;
    }

    private SinkProto.ColumnValue.Builder parseCanonicalValue(
            Object raw, String fieldName, TypeDescription type)
    {
        if (raw == null)
        {
            return SinkProto.ColumnValue.newBuilder()
                    .setValue(ByteString.EMPTY);
        }
        if (raw instanceof JsonNode jsonNode)
        {
            return parseValue(jsonNode, fieldName, type);
        }

        SinkProto.ColumnValue.Builder builder = SinkProto.ColumnValue.newBuilder();
        switch (type.getCategory())
        {
            case BYTE:
                buildInt32(((Number) raw).intValue(), Byte.BYTES, builder);
                break;
            case SHORT:
                buildInt32(((Number) raw).intValue(), Integer.BYTES, builder);
                break;
            case INT:
                buildInt32(((Number) raw).intValue(), Integer.BYTES, builder);
                break;
            case LONG:
            case TIMESTAMP:
            {
                byte[] bytes = ByteBuffer.allocate(Long.BYTES)
                        .putLong(((Number) raw).longValue()).array();
                builder.setValue(ByteString.copyFrom(bytes));
                break;
            }
            case DATE:
            case TIME:
                buildInt32(((Number) raw).intValue(), Integer.BYTES, builder);
                break;
            case CHAR:
            case VARCHAR:
            case STRING:
                builder.setValue(ByteString.copyFrom(raw.toString(), StandardCharsets.UTF_8));
                break;
            case DECIMAL:
                return encodeDecimal(toBigDecimal(raw, type), fieldName, type);
            case BINARY:
            case VARBINARY:
                builder.setValue(ByteString.copyFrom(toBytes(raw)));
                break;
            case DOUBLE:
            {
                long bits = Double.doubleToLongBits(((Number) raw).doubleValue());
                builder.setValue(ByteString.copyFrom(
                        ByteBuffer.allocate(Long.BYTES).putLong(bits).array()));
                break;
            }
            case FLOAT:
                buildFloat32(((Number) raw).floatValue(), builder);
                break;
            case BOOLEAN:
                builder.setValue(ByteString.copyFrom(
                        new byte[]{(byte) ((Boolean) raw ? 1 : 0)}));
                break;
            case STRUCT:
                throw new UnsupportedOperationException("STRUCT parsing not yet implemented");
            default:
                throw new IllegalArgumentException(
                        "Unsupported canonical type: " + type.getCategory());
        }
        return builder;
    }

    private BigDecimal toBigDecimal(Object raw, TypeDescription type)
    {
        if (raw instanceof BigDecimal decimal)
        {
            return decimal;
        }
        if (raw instanceof ByteBuffer buffer)
        {
            return new BigDecimal(new BigInteger(toBytes(buffer)), type.getScale());
        }
        if (raw instanceof byte[] bytes)
        {
            return new BigDecimal(new BigInteger(bytes), type.getScale());
        }
        return new BigDecimal(raw.toString());
    }

    private byte[] toBytes(Object raw)
    {
        if (raw instanceof byte[] bytes)
        {
            return bytes;
        }
        if (raw instanceof ByteBuffer buffer)
        {
            ByteBuffer copy = buffer.duplicate();
            byte[] bytes = new byte[copy.remaining()];
            copy.get(bytes);
            return bytes;
        }
        return raw.toString().getBytes(StandardCharsets.UTF_8);
    }

    BigDecimal parseDecimal(JsonNode node, TypeDescription type)
    {
        byte[] bytes = Base64.getDecoder().decode(node.asText());
        int scale = type.getScale();
        return new BigDecimal(new BigInteger(bytes), scale);
    }

}
