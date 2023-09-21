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
import io.trino.spi.type.Int128;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import static io.trino.hive.formats.avro.NativeLogicalTypesAvroTypeManager.fitBigEndianValueToByteArraySize;
import static io.trino.hive.formats.avro.NativeLogicalTypesAvroTypeManager.fromBigEndian;
import static io.trino.hive.formats.avro.NativeLogicalTypesAvroTypeManager.padBigEndianToSize;
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

    @Test
    public void testWithPadding()
    {
        long a = 780681600000L;
        long b = Long.MIN_VALUE;
        long c = Long.MAX_VALUE;
        long d = 0L;
        long e = -1L;

        for (int i = 9; i < 24; i++) {
            assertThat(fromBigEndian(padBigEndianToSize(a, i))).isEqualTo(a);
            assertThat(fromBigEndian(padBigEndianToSize(b, i))).isEqualTo(b);
            assertThat(fromBigEndian(padBigEndianToSize(c, i))).isEqualTo(c);
            assertThat(fromBigEndian(padBigEndianToSize(d, i))).isEqualTo(d);
            assertThat(fromBigEndian(padBigEndianToSize(e, i))).isEqualTo(e);
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

    @Test
    public void testPad()
    {
        assertThat(padBigEndianToSize(new byte[] {0}, 10)).isEqualTo(new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
        assertThat(padBigEndianToSize(new byte[] {-1}, 10)).isEqualTo(new byte[] {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1});
        assertThat(padBigEndianToSize(new byte[] {(byte) 0x80, 0x00}, 10)).isEqualTo(new byte[] {-1, -1, -1, -1, -1, -1, -1, -1, (byte) 0x80, 0});

        assertThat(padBigEndianToSize(2, 10)).isEqualTo(new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 2});
        assertThat(padBigEndianToSize(0xFF, 10)).isEqualTo(new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, -1});
        assertThat(padBigEndianToSize(Long.MIN_VALUE, 10)).isEqualTo(new byte[] {-1, -1, (byte) 0x80, 0, 0, 0, 0, 0, 0, 0});
        assertThat(padBigEndianToSize(Long.MAX_VALUE, 10)).isEqualTo(new byte[] {0, 0, (byte) 0x7F, -1, -1, -1, -1, -1, -1, -1});

        assertThat(padBigEndianToSize(Int128.valueOf(2), 18)).isEqualTo(new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2});
        assertThat(padBigEndianToSize(Int128.valueOf(-1), 18)).isEqualTo(new byte[] {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1});
        assertThat(padBigEndianToSize(Int128.MIN_VALUE, 18)).isEqualTo(new byte[] {-1, -1, (byte) 0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
        assertThat(padBigEndianToSize(Int128.MAX_VALUE, 18)).isEqualTo(new byte[] {0, 0, 0x7F, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1});
    }

    @Test
    public void testBigEndianResize()
    {
        //test identity
        assertThat(fitBigEndianValueToByteArraySize(2, 8)).isEqualTo(Longs.toByteArray(2));
        assertThat(fitBigEndianValueToByteArraySize(Int128.valueOf(Long.MAX_VALUE), 16)).isEqualTo(Int128.valueOf(Long.MAX_VALUE).toBigEndianBytes());

        // test scale up
        assertThat(fitBigEndianValueToByteArraySize(42, 16)).isEqualTo(Int128.valueOf(42).toBigEndianBytes());
        assertThat(fitBigEndianValueToByteArraySize(-2000, 16)).isEqualTo(Int128.valueOf(-2000).toBigEndianBytes());

        // test scale down
        assertThat(fitBigEndianValueToByteArraySize(Int128.valueOf(32), 8)).isEqualTo(Longs.toByteArray(32));
        assertThat(fitBigEndianValueToByteArraySize(Int128.valueOf(-1), 8)).isEqualTo(Longs.toByteArray(-1));

        assertThat(fitBigEndianValueToByteArraySize(1, 3)).isEqualTo(new byte[] {0, 0, 1});
        assertThat(fitBigEndianValueToByteArraySize(Int128.valueOf(-7), 4)).isEqualTo(new byte[] {-1, -1, -1, -7});
        assertThat(fitBigEndianValueToByteArraySize(new byte[] {2, 4, 5}, 5)).isEqualTo(new byte[] {0, 0, 2, 4, 5});
        assertThat(fitBigEndianValueToByteArraySize(new byte[] {-7, 4, 5}, 6)).isEqualTo(new byte[] {-1, -1, -1, -7, 4, 5});

        //fails size down prereq 1
        assertThatThrownBy(() -> fitBigEndianValueToByteArraySize(new byte[] {0x7F}, 0)).isInstanceOf(ArithmeticException.class);
        assertThatThrownBy(() -> fitBigEndianValueToByteArraySize(new byte[] {0x00, 0x02, (byte) 200, -34}, -3)).isInstanceOf(ArithmeticException.class);

        //fails size down prereq 2
        assertThatThrownBy(() -> fitBigEndianValueToByteArraySize(new byte[] {0x01, 0x02, 0x03}, 2)).isInstanceOf(ArithmeticException.class);
        assertThatThrownBy(() -> fitBigEndianValueToByteArraySize(new byte[] {-2, 0x02, 0x03}, 2)).isInstanceOf(ArithmeticException.class);

        // case 1 resize down assert proper significant bit
        assertThatThrownBy(() -> fitBigEndianValueToByteArraySize(new byte[] {0, 0, -1}, 1)).isInstanceOf(ArithmeticException.class);
        assertThat(fitBigEndianValueToByteArraySize(new byte[] {0, 0, -1}, 2)).isEqualTo(new byte[] {0, -1});

        // case 2 resize down assert proper significant bit
        assertThatThrownBy(() -> fitBigEndianValueToByteArraySize(new byte[] {0, 0, 1}, 0)).isInstanceOf(ArithmeticException.class);
        assertThat(fitBigEndianValueToByteArraySize(new byte[] {0, 0, 1}, 1)).isEqualTo(new byte[] {1});
        assertThat(fitBigEndianValueToByteArraySize(new byte[] {0, 0, 1}, 2)).isEqualTo(new byte[] {0, 1});

        // case 3 resize down assert proper significant bit
        assertThatThrownBy(() -> fitBigEndianValueToByteArraySize(new byte[] {-1, -1, 2}, 1)).isInstanceOf(ArithmeticException.class);
        assertThat(fitBigEndianValueToByteArraySize(new byte[] {-1, -1, 0}, 2)).isEqualTo(new byte[] {-1, 0});

        // case 4 resize down assert proper significant bit
        assertThatThrownBy(() -> fitBigEndianValueToByteArraySize(new byte[] {-1, -1, -34}, 0)).isInstanceOf(ArithmeticException.class);
        assertThat(fitBigEndianValueToByteArraySize(new byte[] {-1, -1, -34}, 1)).isEqualTo(new byte[] {-34});
        assertThat(fitBigEndianValueToByteArraySize(new byte[] {-1, -1, -1}, 2)).isEqualTo(new byte[] {-1, -1});
    }
}
