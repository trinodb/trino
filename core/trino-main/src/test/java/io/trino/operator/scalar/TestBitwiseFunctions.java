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
package io.trino.operator.scalar;

import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestBitwiseFunctions
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testBitCount()
    {
        assertThat(assertions.function("bit_count", "0", "64"))
                .isEqualTo(0L);

        assertThat(assertions.function("bit_count", "7", "64"))
                .isEqualTo(3L);

        assertThat(assertions.function("bit_count", "24", "64"))
                .isEqualTo(2L);

        assertThat(assertions.function("bit_count", "-8", "64"))
                .isEqualTo(61L);

        assertThat(assertions.function("bit_count", Long.toString(Integer.MAX_VALUE), "64"))
                .isEqualTo(31L);

        assertThat(assertions.function("bit_count", Long.toString(Integer.MIN_VALUE), "64"))
                .isEqualTo(33L);

        assertThat(assertions.function("bit_count", Long.toString(Long.MAX_VALUE), "64"))
                .isEqualTo(63L);

        // bit_count(MIN_VALUE, 64)
        assertThat(assertions.function("bit_count", Long.toString(-Long.MAX_VALUE - 1), "64"))
                .isEqualTo(1L);

        assertThat(assertions.function("bit_count", "0", "32"))
                .isEqualTo(0L);

        assertThat(assertions.function("bit_count", "7", "32"))
                .isEqualTo(3L);

        assertThat(assertions.function("bit_count", "24", "32"))
                .isEqualTo(2L);

        assertThat(assertions.function("bit_count", "-8", "32"))
                .isEqualTo(29L);

        assertThat(assertions.function("bit_count", Long.toString(Integer.MAX_VALUE), "32"))
                .isEqualTo(31L);

        assertThat(assertions.function("bit_count", Long.toString(Integer.MIN_VALUE), "32"))
                .isEqualTo(1L);

        assertTrinoExceptionThrownBy(() -> assertions.function("bit_count", Long.toString(Integer.MAX_VALUE + 1L), "32").evaluate())
                .hasMessage("Number must be representable with the bits specified. 2147483648 cannot be represented with 32 bits");

        assertTrinoExceptionThrownBy(() -> assertions.function("bit_count", Long.toString(Integer.MIN_VALUE - 1L), "32").evaluate())
                .hasMessage("Number must be representable with the bits specified. -2147483649 cannot be represented with 32 bits");

        assertThat(assertions.function("bit_count", "1152921504598458367", "62"))
                .isEqualTo(59L);

        assertThat(assertions.function("bit_count", "-1", "62"))
                .isEqualTo(62L);

        assertThat(assertions.function("bit_count", "33554132", "26"))
                .isEqualTo(20L);

        assertThat(assertions.function("bit_count", "-1", "26"))
                .isEqualTo(26L);

        assertTrinoExceptionThrownBy(() -> assertions.function("bit_count", "1152921504598458367", "60").evaluate())
                .hasMessage("Number must be representable with the bits specified. 1152921504598458367 cannot be represented with 60 bits");

        assertTrinoExceptionThrownBy(() -> assertions.function("bit_count", "33554132", "25").evaluate())
                .hasMessage("Number must be representable with the bits specified. 33554132 cannot be represented with 25 bits");

        assertTrinoExceptionThrownBy(() -> assertions.function("bit_count", "0", "-1").evaluate())
                .hasMessage("Bits specified in bit_count must be between 2 and 64, got -1");

        assertTrinoExceptionThrownBy(() -> assertions.function("bit_count", "0", "1").evaluate())
                .hasMessage("Bits specified in bit_count must be between 2 and 64, got 1");

        assertTrinoExceptionThrownBy(() -> assertions.function("bit_count", "0", "65").evaluate())
                .hasMessage("Bits specified in bit_count must be between 2 and 64, got 65");
    }

    @Test
    public void testBitwiseNot()
    {
        assertThat(assertions.function("bitwise_not", "0"))
                .isEqualTo(~0L);

        assertThat(assertions.function("bitwise_not", "-1"))
                .isEqualTo(~-1L);

        assertThat(assertions.function("bitwise_not", "8"))
                .isEqualTo(~8L);

        assertThat(assertions.function("bitwise_not", "-8"))
                .isEqualTo(~-8L);

        assertThat(assertions.function("bitwise_not", Long.toString(Long.MAX_VALUE)))
                .isEqualTo(~Long.MAX_VALUE);

        // bitwise_not(MIN_VALUE)
        assertThat(assertions.function("bitwise_not", Long.toString(-Long.MAX_VALUE - 1)))
                .isEqualTo(~Long.MIN_VALUE);
    }

    @Test
    public void testBitwiseAnd()
    {
        assertThat(assertions.function("bitwise_and", "0", "-1"))
                .isEqualTo(0L);

        assertThat(assertions.function("bitwise_and", "3", "8"))
                .isEqualTo(3L & 8L);

        assertThat(assertions.function("bitwise_and", "-4", "12"))
                .isEqualTo(-4L & 12L);

        assertThat(assertions.function("bitwise_and", "60", "21"))
                .isEqualTo(60L & 21L);
    }

    @Test
    public void testBitwiseOr()
    {
        assertThat(assertions.function("bitwise_or", "0", "-1"))
                .isEqualTo(-1L);

        assertThat(assertions.function("bitwise_or", "3", "8"))
                .isEqualTo(3L | 8L);

        assertThat(assertions.function("bitwise_or", "-4", "12"))
                .isEqualTo(-4L | 12L);

        assertThat(assertions.function("bitwise_or", "60", "21"))
                .isEqualTo(60L | 21L);
    }

    @Test
    public void testBitwiseXor()
    {
        assertThat(assertions.function("bitwise_xor", "0", "-1"))
                .isEqualTo(-1L);

        assertThat(assertions.function("bitwise_xor", "3", "8"))
                .isEqualTo(3L ^ 8L);

        assertThat(assertions.function("bitwise_xor", "-4", "12"))
                .isEqualTo(-4L ^ 12L);

        assertThat(assertions.function("bitwise_xor", "60", "21"))
                .isEqualTo(60L ^ 21L);
    }

    @Test
    public void testBitwiseLeftShift()
    {
        assertThat(assertions.function("bitwise_left_shift", "TINYINT'7'", "2"))
                .isEqualTo((byte) (7 << 2));

        assertThat(assertions.function("bitwise_left_shift", "TINYINT '-7'", "2"))
                .isEqualTo((byte) (-7 << 2));

        assertThat(assertions.function("bitwise_left_shift", "TINYINT '1'", "7"))
                .isEqualTo((byte) (1 << 7));

        assertThat(assertions.function("bitwise_left_shift", "TINYINT '-128'", "1"))
                .isEqualTo((byte) 0);

        assertThat(assertions.function("bitwise_left_shift", "TINYINT '-65'", "1"))
                .isEqualTo((byte) (-65 << 1));

        assertThat(assertions.function("bitwise_left_shift", "TINYINT '-7'", "64"))
                .isEqualTo((byte) 0);

        assertThat(assertions.function("bitwise_left_shift", "TINYINT '-128'", "0"))
                .isEqualTo((byte) -128);

        assertThat(assertions.function("bitwise_left_shift", "SMALLINT '7'", "2"))
                .isEqualTo((short) (7 << 2));

        assertThat(assertions.function("bitwise_left_shift", "SMALLINT '-7'", "2"))
                .isEqualTo((short) (-7 << 2));

        assertThat(assertions.function("bitwise_left_shift", "SMALLINT '1'", "7"))
                .isEqualTo((short) (1 << 7));

        assertThat(assertions.function("bitwise_left_shift", "SMALLINT '-32768'", "1"))
                .isEqualTo((short) 0);

        assertThat(assertions.function("bitwise_left_shift", "SMALLINT '-65'", "1"))
                .isEqualTo((short) (-65 << 1));

        assertThat(assertions.function("bitwise_left_shift", "SMALLINT '-7'", "64"))
                .isEqualTo((short) 0);

        assertThat(assertions.function("bitwise_left_shift", "SMALLINT '-32768'", "0"))
                .isEqualTo((short) -32768);

        assertThat(assertions.function("bitwise_left_shift", "INTEGER '7'", "2"))
                .isEqualTo(7 << 2);

        assertThat(assertions.function("bitwise_left_shift", "INTEGER '-7'", "2"))
                .isEqualTo(-7 << 2);

        assertThat(assertions.function("bitwise_left_shift", "INTEGER '1'", "7"))
                .isEqualTo(1 << 7);

        assertThat(assertions.function("bitwise_left_shift", "INTEGER '-2147483648'", "1"))
                .isEqualTo(0);

        assertThat(assertions.function("bitwise_left_shift", "INTEGER '-65'", "1"))
                .isEqualTo(-65 << 1);

        assertThat(assertions.function("bitwise_left_shift", "INTEGER '-7'", "64"))
                .isEqualTo(0);

        assertThat(assertions.function("bitwise_left_shift", "INTEGER '-2147483648'", "0"))
                .isEqualTo(-2147483648);

        assertThat(assertions.function("bitwise_left_shift", "BIGINT '7'", "2"))
                .isEqualTo(7L << 2);

        assertThat(assertions.function("bitwise_left_shift", "BIGINT '-7'", "2"))
                .isEqualTo(-7L << 2);

        assertThat(assertions.function("bitwise_left_shift", "BIGINT '-7'", "64"))
                .isEqualTo(0L);
    }

    @Test
    public void testBitwiseRightShift()
    {
        assertThat(assertions.function("bitwise_right_shift", "TINYINT '7'", "2"))
                .isEqualTo((byte) (7 >>> 2));

        assertThat(assertions.function("bitwise_right_shift", "TINYINT '-7'", "2"))
                .isEqualTo((byte) 62);

        assertThat(assertions.function("bitwise_right_shift", "TINYINT '-7'", "64"))
                .isEqualTo((byte) 0);

        assertThat(assertions.function("bitwise_right_shift", "TINYINT '-128'", "0"))
                .isEqualTo((byte) -128);

        assertThat(assertions.function("bitwise_right_shift", "SMALLINT '7'", "2"))
                .isEqualTo((short) (7 >>> 2));

        assertThat(assertions.function("bitwise_right_shift", "SMALLINT '-7'", "2"))
                .isEqualTo((short) 16382);

        assertThat(assertions.function("bitwise_right_shift", "SMALLINT '-7'", "64"))
                .isEqualTo((short) 0);

        assertThat(assertions.function("bitwise_right_shift", "SMALLINT '-32768'", "0"))
                .isEqualTo((short) -32768);

        assertThat(assertions.function("bitwise_right_shift", "INTEGER '7'", "2"))
                .isEqualTo(7 >>> 2);

        assertThat(assertions.function("bitwise_right_shift", "INTEGER '-7'", "2"))
                .isEqualTo(1073741822);

        assertThat(assertions.function("bitwise_right_shift", "INTEGER '-7'", "64"))
                .isEqualTo(0);

        assertThat(assertions.function("bitwise_right_shift", "INTEGER '-2147483648'", "0"))
                .isEqualTo(-2147483648);

        assertThat(assertions.function("bitwise_right_shift", "BIGINT '7'", "2"))
                .isEqualTo(7L >>> 2);

        assertThat(assertions.function("bitwise_right_shift", "BIGINT '-7'", "2"))
                .isEqualTo(-7L >>> 2);

        assertThat(assertions.function("bitwise_right_shift", "BIGINT '-7'", "64"))
                .isEqualTo(0L);
    }

    @Test
    public void testBitwiseRightShiftArithmetic()
    {
        assertThat(assertions.function("bitwise_right_shift_arithmetic", "TINYINT '7'", "2"))
                .isEqualTo((byte) (7 >> 2));

        assertThat(assertions.function("bitwise_right_shift_arithmetic", "TINYINT '-7'", "2"))
                .isEqualTo((byte) (-7 >> 2));

        assertThat(assertions.function("bitwise_right_shift_arithmetic", "TINYINT '7'", "64"))
                .isEqualTo((byte) 0);

        assertThat(assertions.function("bitwise_right_shift_arithmetic", "TINYINT '-7'", "64"))
                .isEqualTo((byte) -1);

        assertThat(assertions.function("bitwise_right_shift_arithmetic", "TINYINT '-128'", "0"))
                .isEqualTo((byte) -128);

        assertThat(assertions.function("bitwise_right_shift_arithmetic", "SMALLINT '7'", "2"))
                .isEqualTo((short) (7 >> 2));

        assertThat(assertions.function("bitwise_right_shift_arithmetic", "SMALLINT '-7'", "2"))
                .isEqualTo((short) (-7 >> 2));

        assertThat(assertions.function("bitwise_right_shift_arithmetic", "SMALLINT '7'", "64"))
                .isEqualTo((short) 0);

        assertThat(assertions.function("bitwise_right_shift_arithmetic", "SMALLINT '-7'", "64"))
                .isEqualTo((short) -1);

        assertThat(assertions.function("bitwise_right_shift_arithmetic", "SMALLINT '-32768'", "0"))
                .isEqualTo((short) -32768);

        assertThat(assertions.function("bitwise_right_shift_arithmetic", "INTEGER '7'", "2"))
                .isEqualTo(7 >> 2);

        assertThat(assertions.function("bitwise_right_shift_arithmetic", "INTEGER '-7'", "2"))
                .isEqualTo(-7 >> 2);

        assertThat(assertions.function("bitwise_right_shift_arithmetic", "INTEGER '7'", "64"))
                .isEqualTo(0);

        assertThat(assertions.function("bitwise_right_shift_arithmetic", "INTEGER '-7'", "64"))
                .isEqualTo(-1);

        assertThat(assertions.function("bitwise_right_shift_arithmetic", "INTEGER '-2147483648'", "0"))
                .isEqualTo(-2147483648);

        assertThat(assertions.function("bitwise_right_shift_arithmetic", "BIGINT '7'", "2"))
                .isEqualTo(7L >> 2);

        assertThat(assertions.function("bitwise_right_shift_arithmetic", "BIGINT '-7'", "2"))
                .isEqualTo(-7L >> 2);

        assertThat(assertions.function("bitwise_right_shift_arithmetic", "BIGINT '7'", "64"))
                .isEqualTo(0L);

        assertThat(assertions.function("bitwise_right_shift_arithmetic", "BIGINT '-7'", "64"))
                .isEqualTo(-1L);
    }
}
