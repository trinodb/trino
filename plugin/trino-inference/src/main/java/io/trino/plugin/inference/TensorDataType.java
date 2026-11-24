/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.inference;

import com.google.common.base.Enums;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.Optional;

import static io.trino.plugin.inference.InferenceErrorCode.INFERENCE_INVALID_RESPONSE;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;

/**
 * Tensor data types defined by the Open Inference Protocol (V2), and their Trino equivalents.
 * Unsigned types map to the next wider signed type. {@code UINT64} is not supported,
 * because its values do not fit in {@code BIGINT}.
 */
public enum TensorDataType
{
    BOOL(BOOLEAN),
    INT8(TINYINT),
    INT16(SMALLINT),
    INT32(INTEGER),
    INT64(BIGINT),
    UINT8(SMALLINT),
    UINT16(INTEGER),
    UINT32(BIGINT),
    FP16(REAL),
    FP32(REAL),
    FP64(DOUBLE),
    BYTES(VARCHAR);

    private final Type trinoType;

    TensorDataType(Type trinoType)
    {
        this.trinoType = trinoType;
    }

    public static Optional<TensorDataType> fromName(String name)
    {
        return Enums.getIfPresent(TensorDataType.class, name).toJavaUtil();
    }

    public Type trinoType()
    {
        return trinoType;
    }

    public boolean accepts(Type type)
    {
        if (this == BYTES) {
            return type instanceof VarcharType;
        }
        return trinoType.equals(type);
    }

    /**
     * Reads a non-null value of an input column from a block of a type accepted by this data type, in the form used for JSON encoding.
     */
    public Object readValue(Block block, int position, String column)
    {
        return switch (this) {
            case BOOL -> BOOLEAN.getBoolean(block, position);
            case INT8, INT16, INT32, INT64 -> trinoType.getLong(block, position);
            case UINT8 -> checkUnsigned(trinoType.getLong(block, position), 0xFFL, column);
            case UINT16 -> checkUnsigned(trinoType.getLong(block, position), 0xFFFFL, column);
            case UINT32 -> checkUnsigned(trinoType.getLong(block, position), 0xFFFF_FFFFL, column);
            case FP16, FP32 -> checkFinite(REAL.getFloat(block, position), column);
            case FP64 -> checkFinite(DOUBLE.getDouble(block, position), column);
            case BYTES -> VARCHAR.getSlice(block, position).toStringUtf8();
        };
    }

    // the JSON encoding of the protocol cannot represent NaN and infinity

    private static <T extends Number> T checkFinite(T value, String column)
    {
        if (!Double.isFinite(value.doubleValue())) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Column %s has the value %s, which cannot be sent to the model server, because JSON does not support NaN and infinite numbers. Remove or replace these values in the input query".formatted(column, value));
        }
        return value;
    }

    private long checkUnsigned(long value, long maxValue, String column)
    {
        if (value < 0 || value > maxValue) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Column %s has the value %s, which is out of range for the data type %s of the model input. Remove or replace these values in the input query".formatted(column, value, this));
        }
        return value;
    }

    /**
     * Writes a value of an output, decoded from JSON, to a block builder of this data type's Trino type.
     *
     * @param output the description of the output, used in error messages
     */
    public void writeValue(BlockBuilder blockBuilder, Object value, String output)
    {
        if (value == null) {
            blockBuilder.appendNull();
            return;
        }
        switch (this) {
            case BOOL -> {
                if (value instanceof Boolean booleanValue) {
                    BOOLEAN.writeBoolean(blockBuilder, booleanValue);
                }
                else {
                    BOOLEAN.writeBoolean(blockBuilder, toNumber(value, output).longValue() != 0);
                }
            }
            case INT8, INT16, INT32, INT64, UINT8, UINT16, UINT32 -> trinoType.writeLong(blockBuilder, toLong(value, output));
            case FP16, FP32 -> REAL.writeFloat(blockBuilder, toNumber(value, output).floatValue());
            case FP64 -> DOUBLE.writeDouble(blockBuilder, toNumber(value, output).doubleValue());
            case BYTES -> VARCHAR.writeString(blockBuilder, value.toString());
        }
    }

    private long toLong(Object value, String output)
    {
        // JSON integers are decoded to Integer or Long, or to BigInteger when they do not fit in a Long
        if (value instanceof Integer || value instanceof Long) {
            return ((Number) value).longValue();
        }
        throw invalidValue(value, output, "integers");
    }

    private Number toNumber(Object value, String output)
    {
        if (value instanceof Number number) {
            return number;
        }
        throw invalidValue(value, output, "numbers");
    }

    private TrinoException invalidValue(Object value, String output, String expected)
    {
        // JSON numbers with a fraction or an exponent are decoded to Double, also when their value is integral, such as 1.0
        String description = "the value " + value;
        if (value instanceof Double) {
            description = "the floating-point number " + value;
        }
        return new TrinoException(INFERENCE_INVALID_RESPONSE, "Model server returned %s for the %s, but the data type %s of the output requires %s. Declare the data type of the output that the model returns in the configuration of the model on the model server".formatted(
                description,
                output,
                this,
                expected));
    }
}
