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
package io.trino.hive.formats.avro;

import com.google.common.primitives.Longs;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.base.Verify.verify;
import static io.trino.hive.formats.avro.NativeLogicalTypesAvroTypeManager.fromBigEndian;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class TestLongFromBigEndian
{
    @Test
    public void testArrays()
    {
        assertThat(fromBigEndian(new byte[] {(byte) 0xFF, (byte) 0xFF})).isEqualTo(-1);
        assertThat(fromBigEndian(new byte[] {0, 0, 0, 0, 0, 0, (byte) 0xFF, (byte) 0xFF})).isEqualTo(65535);
        assertThat(fromBigEndian(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0x80, 0, 0, 0, 0, 0, 0, 0})).isEqualTo(Long.MIN_VALUE);
    }

    @Test
    public void testIdentity()
    {
        long a = 780681600000L;
        long b = Long.MIN_VALUE;
        long c = Long.MAX_VALUE;
        long d = 0L;
        long e = -1L;

        assertThat(fromBigEndian(Longs.toByteArray(a))).isEqualTo(a);
        assertThat(fromBigEndian(Longs.toByteArray(b))).isEqualTo(b);
        assertThat(fromBigEndian(Longs.toByteArray(c))).isEqualTo(c);
        assertThat(fromBigEndian(Longs.toByteArray(d))).isEqualTo(d);
        assertThat(fromBigEndian(Longs.toByteArray(e))).isEqualTo(e);
    }

    @Test
    public void testLessThan8Bytes()
    {
        long a = 24L;
        long b = -24L;
        long c = 0L;
        long d = 1L;
        long e = -1L;
        long f = 64L;
        long g = -64L;

        for (int i = 0; i < 8; i++) {
            assertThat(fromBigEndian(Arrays.copyOfRange(Longs.toByteArray(a), i, 8))).isEqualTo(a);
            assertThat(fromBigEndian(Arrays.copyOfRange(Longs.toByteArray(b), i, 8))).isEqualTo(b);
            assertThat(fromBigEndian(Arrays.copyOfRange(Longs.toByteArray(c), i, 8))).isEqualTo(c);
            assertThat(fromBigEndian(Arrays.copyOfRange(Longs.toByteArray(d), i, 8))).isEqualTo(d);
            assertThat(fromBigEndian(Arrays.copyOfRange(Longs.toByteArray(e), i, 8))).isEqualTo(e);
            assertThat(fromBigEndian(Arrays.copyOfRange(Longs.toByteArray(f), i, 8))).isEqualTo(f);
            assertThat(fromBigEndian(Arrays.copyOfRange(Longs.toByteArray(g), i, 8))).isEqualTo(g);
        }
    }

    public static byte[] padBigEndianCorrectly(long toPad, int totalSize)
    {
        verify(totalSize >= 8);
        byte[] longBytes = Longs.toByteArray(toPad);

        byte[] padded = new byte[totalSize];

        System.arraycopy(longBytes, 0, padded, totalSize - 8, 8);
        if (toPad < 0) {
            for (int i = 0; i < totalSize - 8; i++) {
                padded[i] = -1;
            }
        }
        return padded;
    }

    @Test
    public void testWithPadding()
    {
        long a = 780681600000L;
        long b = Long.MIN_VALUE;
        long c = Long.MAX_VALUE;
        long d = 0L;
        long e = -1L;

        for (int i = 9; i < 24; i++) {
            assertThat(fromBigEndian(padBigEndianCorrectly(a, i))).isEqualTo(a);
            assertThat(fromBigEndian(padBigEndianCorrectly(b, i))).isEqualTo(b);
            assertThat(fromBigEndian(padBigEndianCorrectly(c, i))).isEqualTo(c);
            assertThat(fromBigEndian(padBigEndianCorrectly(d, i))).isEqualTo(d);
            assertThat(fromBigEndian(padBigEndianCorrectly(e, i))).isEqualTo(e);
        }
    }

    private static byte[] padPoorly(long toPad)
    {
        int totalSize = 32;
        byte[] longBytes = Longs.toByteArray(toPad);

        byte[] padded = new byte[totalSize];

        System.arraycopy(longBytes, 0, padded, totalSize - 8, 8);

        for (int i = 0; i < totalSize - 8; i++) {
            padded[i] = ThreadLocalRandom.current().nextBoolean() ? (byte) ThreadLocalRandom.current().nextInt(1, Byte.MAX_VALUE) : (byte) ThreadLocalRandom.current().nextInt(Byte.MIN_VALUE, -1);
        }

        return padded;
    }

    @Test
    public void testWithBadPadding()
    {
        long a = 780681600000L;
        long b = Long.MIN_VALUE;
        long c = Long.MAX_VALUE;
        long d = 0L;
        long e = -1L;

        assertThatThrownBy(() -> fromBigEndian(padPoorly(a))).isInstanceOf(ArithmeticException.class);
        assertThatThrownBy(() -> fromBigEndian(padPoorly(b))).isInstanceOf(ArithmeticException.class);
        assertThatThrownBy(() -> fromBigEndian(padPoorly(c))).isInstanceOf(ArithmeticException.class);
        assertThatThrownBy(() -> fromBigEndian(padPoorly(d))).isInstanceOf(ArithmeticException.class);
        assertThatThrownBy(() -> fromBigEndian(padPoorly(e))).isInstanceOf(ArithmeticException.class);
    }
}
