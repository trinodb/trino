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
package io.trino.plugin.pinot.decoders;

import io.trino.plugin.pinot.PinotException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.FixedWidthType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.Type;

import java.util.Optional;

import static io.trino.plugin.pinot.PinotErrorCode.PINOT_UNSUPPORTED_COLUMN_TYPE;
import static java.util.Objects.requireNonNull;

public class DecoderFactory
{
    private DecoderFactory()
    {
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
