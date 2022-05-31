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
package io.trino.execution.buffer;

import io.trino.spi.type.DecimalType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.Type;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class BenchmarkDataGenerator
{
    public static final int MAX_STRING = 19;
    public static final int LONG_DECIMAL_PRECISION = 30;
    public static final int LONG_DECIMAL_SCALE = 5;

    private BenchmarkDataGenerator()
    {
    }

    public static <T> Iterator<T> createValues(int size, Function<Random, T> valueGenerator, double nullChance)
    {
        Random random = new Random(0);
        List<T> values = new ArrayList<>();
        for (int i = 0; i < size; ++i) {
            if (randomNullChance(random) >= nullChance) {
                values.add(valueGenerator.apply(random));
            }
            else {
                values.add(null);
            }
        }
        return values.iterator();
    }

    private static double randomNullChance(Random random)
    {
        double value = 0;
        // null chance has to be 0 to 1 exclusive.
        while (value == 0) {
            value = random.nextDouble();
        }
        return value;
    }

    public static String randomAsciiString(Random random)
    {
        return randomAsciiString(MAX_STRING, random);
    }

    public static String randomAsciiString(int maxLength, Random random)
    {
        char[] value = new char[random.nextInt(maxLength)];
        for (int i = 0; i < value.length; i++) {
            value[i] = (char) random.nextInt(Byte.MAX_VALUE);
        }
        return new String(value);
    }

    public static SqlDecimal randomLongDecimal(Random random)
    {
        return new SqlDecimal(new BigInteger(96, random), LONG_DECIMAL_PRECISION, LONG_DECIMAL_SCALE);
    }

    public static LongTimestamp randomTimestamp(Random random)
    {
        return new LongTimestamp(random.nextLong(), random.nextInt(PICOSECONDS_PER_MICROSECOND));
    }

    public static short randomShort(Random random)
    {
        return (short) random.nextInt();
    }

    public static byte randomByte(Random random)
    {
        return (byte) random.nextInt();
    }

    public static List<Object> randomRow(List<Type> fieldTypes, Random random)
    {
        List<Object> row = new ArrayList<>(fieldTypes.size());
        for (Type type : fieldTypes) {
            if (type == VARCHAR) {
                row.add(randomAsciiString(random));
            }
            else if (type == BIGINT) {
                row.add(random.nextLong());
            }
            else if (type instanceof DecimalType) {
                row.add(randomLongDecimal(random));
            }
            else {
                throw new UnsupportedOperationException(String.format("The %s is not supported", type));
            }
        }
        return row;
    }
}
