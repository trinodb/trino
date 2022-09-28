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
package io.trino.spi.type;

import org.testng.annotations.Test;

import java.math.BigInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestInt128
{
    @Test
    public void testFromBigEndian()
    {
        byte[] bytes;

        // less than 8 bytes
        bytes = new byte[] {0x1};
        assertThat(Int128.fromBigEndian(bytes))
                .isEqualTo(Int128.valueOf(0x0000000000000000L, 0x0000000000000001L))
                .isEqualTo(Int128.valueOf(new BigInteger(bytes)));

        bytes = new byte[] {(byte) 0xFF};
        assertThat(Int128.fromBigEndian(bytes))
                .isEqualTo(Int128.valueOf(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL))
                .isEqualTo(Int128.valueOf(new BigInteger(bytes)));

        // 8 bytes
        bytes = new byte[] {0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8};
        assertThat(Int128.fromBigEndian(bytes))
                .isEqualTo(Int128.valueOf(0x0000000000000000L, 0x01_02_03_04_05_06_07_08L))
                .isEqualTo(Int128.valueOf(new BigInteger(bytes)));

        bytes = new byte[] {(byte) 0x80, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8};
        assertThat(Int128.fromBigEndian(bytes))
                .isEqualTo(Int128.valueOf(0xFFFFFFFFFFFFFFFFL, 0x80_02_03_04_05_06_07_08L))
                .isEqualTo(Int128.valueOf(new BigInteger(bytes)));

        // more than 8 bytes, less than 16 bytes
        bytes = new byte[] {0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xA};
        assertThat(Int128.fromBigEndian(bytes))
                .isEqualTo(Int128.valueOf(0x000000000000_01_02L, 0x03_04_05_06_07_08_09_0AL))
                .isEqualTo(Int128.valueOf(new BigInteger(bytes)));

        bytes = new byte[] {(byte) 0x80, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xA};
        assertThat(Int128.fromBigEndian(bytes))
                .isEqualTo(Int128.valueOf(0xFFFFFFFFFFFF_80_02L, 0x03_04_05_06_07_08_09_0AL))
                .isEqualTo(Int128.valueOf(new BigInteger(bytes)));

        // 16 bytes
        bytes = new byte[] {0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xA, 0xB, 0xC, 0xD, 0xE, 0xF, 0x55};
        assertThat(Int128.fromBigEndian(bytes))
                .isEqualTo(Int128.valueOf(0x01_02_03_04_05_06_07_08L, 0x09_0A_0B_0C_0D_0E_0F_55L))
                .isEqualTo(Int128.valueOf(new BigInteger(bytes)));

        bytes = new byte[] {(byte) 0x80, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xA, 0xB, 0xC, 0xD, 0xE, 0xF, 0x55};
        assertThat(Int128.fromBigEndian(bytes))
                .isEqualTo(Int128.valueOf(0x80_02_03_04_05_06_07_08L, 0x09_0A_0B_0C_0D_0E_0F_55L))
                .isEqualTo(Int128.valueOf(new BigInteger(bytes)));

        // more than 16 bytes
        bytes = new byte[] {0x0, 0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xA, 0xB, 0xC, 0xD, 0xE, 0xF, 0x55};
        assertThat(Int128.fromBigEndian(bytes))
                .isEqualTo(Int128.valueOf(0x01_02_03_04_05_06_07_08L, 0x09_0A_0B_0C_0D_0E_0F_55L))
                .isEqualTo(Int128.valueOf(new BigInteger(bytes)));

        bytes = new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0x80, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xA, 0xB, 0xC, 0xD, 0xE, 0xF, 0x55};
        assertThat(Int128.fromBigEndian(bytes))
                .isEqualTo(Int128.valueOf(0x80_02_03_04_05_06_07_08L, 0x09_0A_0B_0C_0D_0E_0F_55L))
                .isEqualTo(Int128.valueOf(new BigInteger(bytes)));

        // overflow
        assertThatThrownBy(() -> Int128.fromBigEndian(new byte[] {0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}))
                .isInstanceOf(ArithmeticException.class);

        assertThatThrownBy(() -> Int128.fromBigEndian(new byte[] {(byte) 0xFE, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}))
                .isInstanceOf(ArithmeticException.class);
    }
}
