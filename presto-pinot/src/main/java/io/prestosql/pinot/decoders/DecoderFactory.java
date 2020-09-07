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
package io.prestosql.pinot.decoders;

import io.prestosql.pinot.PinotException;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.FixedWidthType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.Type;

import java.util.Optional;

import static io.prestosql.pinot.PinotErrorCode.PINOT_DECODE_ERROR;
import static io.prestosql.pinot.PinotErrorCode.PINOT_UNSUPPORTED_COLUMN_TYPE;
import static java.util.Objects.requireNonNull;

public class DecoderFactory
{
    private DecoderFactory()
    {
    }

    protected static final String PINOT_INFINITY = "∞";
    protected static final String PINOT_POSITIVE_INFINITY = "+" + PINOT_INFINITY;
    protected static final String PINOT_NEGATIVE_INFINITY = "-" + PINOT_INFINITY;

    protected static final Double PRESTO_INFINITY = Double.POSITIVE_INFINITY;
    protected static final Double PRESTO_NEGATIVE_INFINITY = Double.NEGATIVE_INFINITY;

    public static Double parseDouble(String value)
    {
        try {
            requireNonNull(value, "value is null");
            return Double.valueOf(value);
        }
        catch (NumberFormatException ne) {
            switch (value) {
                case PINOT_INFINITY:
                case PINOT_POSITIVE_INFINITY:
                    return PRESTO_INFINITY;
                case PINOT_NEGATIVE_INFINITY:
                    return PRESTO_NEGATIVE_INFINITY;
            }
            throw new PinotException(PINOT_DECODE_ERROR, Optional.empty(), "Cannot decode double value from pinot " + value, ne);
        }
    }

    public static Decoder createDecoder(Type type)
    {
        requireNonNull(type, "type is null");
        if (type instanceof FixedWidthType) {
            if (type instanceof DoubleType) {
                return new DoubleDecoder();
            }
            else if (type instanceof RealType) {
                return new RealDecoder();
            }
            else if (type instanceof BigintType) {
                return new BigintDecoder();
            }
            else if (type instanceof IntegerType) {
                return new IntegerDecoder();
            }
            else if (type instanceof BooleanType) {
                return new BooleanDecoder();
            }
            else {
                throw new PinotException(PINOT_UNSUPPORTED_COLUMN_TYPE, Optional.empty(), "type '" + type + "' not supported");
            }
        }
        else if (type instanceof ArrayType) {
            return new ArrayDecoder(type);
        }
        else {
            return new VarcharDecoder();
        }
    }
}
