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
package io.trino.plugin.varada.util;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.varada.VaradaErrorCode;
import io.trino.plugin.varada.type.TypeUtils;
import io.trino.spi.TrinoException;
import io.trino.spi.type.CharType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Function;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

public class SliceUtils
{
    private static final Logger logger = Logger.get(SliceUtils.class);

    private SliceUtils()
    {
    }

    public static long str2int(Slice value, boolean isPrefix)
    {
        return str2int(value.toByteBuffer(), value.length(), isPrefix);
    }

    public static long str2int(ByteBuffer byteBuffer, int length, boolean isPrefix)
    {
        int ix;
        int max = Long.BYTES;
        int n = Math.min(length, max);
        long res = 0;

        byteBuffer.position(0);

        if (isPrefix) {
            int pos = byteBuffer.position();

            for (ix = 0; ix < n; ix++) {
                res <<= 8;
                int bufValue = ((int) byteBuffer.get(pos + ix) & 0xff);
                res |= bufValue;
            }
        }
        else {
            int pos = byteBuffer.position() + length - 1;

            for (ix = 0; ix < n; ix++) {
                res <<= 8;
                int bufValue = ((int) byteBuffer.get(pos - ix) & 0xff);
                res |= bufValue;
            }
        }

        if (ix < max) {
            res <<= (Byte.SIZE * (max - ix));
        }

        return res;
    }

    private static int hashCodeVersion1(ByteBuffer a, int length)
    {
        if (a == null) {
            return 0;
        }

        int result = 1;
        for (int ix = a.position(); ix < a.position() + length; ix++) {
            result = 31 * result + a.get(ix);
        }
        return result;
    }

    private static int hashCodeVersion2(ByteBuffer a, int length)
    {
        if (a == null) {
            return 0;
        }

        int result = 503;
        for (int ix = a.position(); ix < a.position() + length; ix++) {
            result = 127 * result + a.get(ix);
        }

        return result;
    }

    public static long calcCrc(Slice currBytes)
    {
        return calcCrc(currBytes.toByteBuffer(), currBytes.length());
    }

    public static long calcCrc(ByteBuffer byteBuffer, int length)
    {
        // NOTE - this functions has a duplication in C layer, if you decide to change it be sure to change the C one also (calc_crc in vrd_primitives.h)
        long h1 = hashCodeVersion1(byteBuffer, length);
        long h2 = hashCodeVersion2(byteBuffer, length);

        return (h2 << 32) | (h1 & 0x00000000ffffffffL);
    }

    // is crc is true we calculate crc, if its false we calculate str2int
    public static long calcStringValue(ByteBuffer byteBuffer, int length, int weRecLength, boolean crc)
    {
        return switch (weRecLength) {
            case 1 -> byteBuffer.get(0);
            case 2 -> byteBuffer.duplicate().order(ByteOrder.LITTLE_ENDIAN).getShort(0);
            case 4 -> byteBuffer.duplicate().order(ByteOrder.LITTLE_ENDIAN).getInt(0);
            default -> {
                if (crc) {
                    yield calcCrc(byteBuffer, length);
                }
                else {
                    yield str2int(byteBuffer, length, true);
                }
            }
        };
    }

    public static Function<Slice, Slice> getSliceConverter(Type type,
            int weLength,
            boolean isFixedLength,
            boolean isValidateSize)
    {
        if (weLength <= 0) {
            throw new AssertionError("Slice length must be positive");
        }
        if (TypeUtils.isArrayType(type)) {
            return Function.identity();
        }
        if (TypeUtils.isMapType(type)) {
            return getSliceConverter(((MapType) type).getValueType(), weLength, isFixedLength, isValidateSize);
        }
        if (TypeUtils.isVarcharType(type) && !isFixedLength) {
            // Long VARCHAR
            // We need to cut the string in case its larger than expected length or the maximal limit of the storage engine
            // Then we need to trim the zeroes from the suffix
            return value -> {
                if (isValidateSize && (weLength < value.length())) {
                    throw new TrinoException(VaradaErrorCode.VARADA_DATA_WARMUP_RULE_ILLEGAL_CHAR_LENGTH,
                            format("Mismatch in slice length[%d] and expected col length[%d], col type[%s]",
                                    value.length(), weLength, type));
                }
                // fail if slice bytes are longer than expected

                Slice result = value;
                int length = value.length();
                int newLength = length;
                newLength = trimSlice(value.toByteBuffer(), newLength, 0);

                if (newLength != length) {
                    result = value.slice(0, newLength);
                }
                return result;
            };
        }
        // CHARs and short VARCHARs are handled differently.
        if (TypeUtils.isCharType(type)) {
            if (((CharType) type).getLength() != weLength) {
                logger.warn("type length=%d is not equal to length=%d", ((CharType) type).getLength(), weLength);
            }

            // CHAR type has weird padding semantics - see VDB-1149.
            // For now, we trim the trailing spaces, similar to MySQL and Postgres.
            return value -> {
                // fail if slice bytes are longer than expected
                if (isValidateSize && (weLength < value.length())) {
                    throw new TrinoException(VaradaErrorCode.VARADA_DATA_WARMUP_RULE_ILLEGAL_CHAR_LENGTH,
                            format("Mismatch in slice length[%d] and expected col length[%d], col type[%s]",
                                    value.length(), weLength, type));
                }
                ArrayUtils.replaceSuffix(value, (byte) ' ', (byte) 0);
                // Pad/trim slice to specific length (with null bytes) if needed
                if (isFixedLength) {
                    return ensureLength(value, weLength);
                }
                return value;
            };
        }
        // Short VARCHARs are padded with nulls to their specified length.
        return value -> ensureLength(value, weLength);
    }

    public static Slice ensureLength(Slice value, int length)
    {
        if (length > value.length()) {
            return Slices.wrappedBuffer(ArrayUtils.copyArray(value.getBytes(), length, (byte) 0));
        }
        return value;
    }

    public static byte[] slice2ByteArray(Slice value, int length)
    {
        byte[] valueInBytes = value.getBytes();
        if (length > valueInBytes.length) {
            return ArrayUtils.copyArray(valueInBytes, length, (byte) 0);
        }
        return valueInBytes;
    }

    public static int trimSlice(ByteBuffer recordBuff, int length, int baseOffset)
    {
        if (length == 0) {
            return length;
        }

        byte currByte = recordBuff.get(baseOffset + length - 1);
        while (currByte == (byte) 0) {
            length--;
            if (length == 0) {
                break;
            }
            currByte = recordBuff.get(baseOffset + length - 1);
        }
        return length;
    }

    public static String serializeSlice(Slice slice)
    {
        return new String(slice.getBytes(), UTF_8);
    }

    public static class StringPredicateDataFactory
    {
        public StringPredicateData create(Slice value, int weRecLength, boolean crc)
        {
            ByteBuffer byteBuffer = value.toByteBuffer();
            /* is case of special char len, we use values instead of crc since crc is not used in C layer */
            return new StringPredicateData(byteBuffer, value.length(),
                    calcStringValue(byteBuffer, value.length(), weRecLength, crc));
        }
    }
}
