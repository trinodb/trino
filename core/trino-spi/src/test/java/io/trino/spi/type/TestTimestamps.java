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

import org.junit.jupiter.api.Test;

import static io.trino.spi.type.Timestamps.round;
import static io.trino.spi.type.Timestamps.roundDiv;
import static io.trino.spi.type.Timestamps.roundExact;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestTimestamps
{
    @Test
    public void testRoundExact()
    {
        // agrees with round() wherever round() does not overflow
        for (int magnitude = 0; magnitude <= 6; magnitude++) {
            for (long value : new long[] {0, 1, -1, 499, 500, 501, -499, -500, -501, 999999, -999999, 44444, -55556, 1234567890123L, -1234567890123L}) {
                assertThat(roundExact(value, magnitude))
                        .as("value %s, magnitude %s", value, magnitude)
                        .isEqualTo(round(value, magnitude));
            }
        }

        // ties round half-up toward positive infinity
        assertThat(roundExact(150, 2)).isEqualTo(200);
        assertThat(roundExact(-150, 2)).isEqualTo(-100);
        assertThat(roundExact(-151, 2)).isEqualTo(-200);

        // rounding near the long range limits succeeds when the result is representable
        assertThat(roundExact(Long.MAX_VALUE, 2)).isEqualTo(9223372036854775800L);
        assertThat(roundExact(Long.MIN_VALUE, 2)).isEqualTo(-9223372036854775800L);

        // rounding fails instead of wrapping when the result exceeds the long range
        assertThatThrownBy(() -> roundExact(Long.MAX_VALUE, 1)).isExactlyInstanceOf(ArithmeticException.class);
        assertThatThrownBy(() -> roundExact(Long.MAX_VALUE, 3)).isExactlyInstanceOf(ArithmeticException.class);
        assertThatThrownBy(() -> roundExact(Long.MIN_VALUE, 1)).isExactlyInstanceOf(ArithmeticException.class);
        assertThatThrownBy(() -> roundExact(Long.MIN_VALUE, 3)).isExactlyInstanceOf(ArithmeticException.class);
    }

    @Test
    public void testRoundDiv()
    {
        // round towards negative infinity
        assertThat(roundDiv(44444, 1)).isEqualTo(44444);
        assertThat(roundDiv(44444, 10)).isEqualTo(4444);
        assertThat(roundDiv(44444, 100)).isEqualTo(444);
        assertThat(roundDiv(44444, 1000)).isEqualTo(44);
        assertThat(roundDiv(44444, 10000)).isEqualTo(4);
        assertThat(roundDiv(44444, 100000)).isEqualTo(0);

        assertThat(roundDiv(-55556, 1)).isEqualTo(-55556);
        assertThat(roundDiv(-55556, 10)).isEqualTo(-5556);
        assertThat(roundDiv(-55556, 100)).isEqualTo(-556);
        assertThat(roundDiv(-55556, 1000)).isEqualTo(-56);
        assertThat(roundDiv(-55556, 10000)).isEqualTo(-6);
        assertThat(roundDiv(-55556, 100000)).isEqualTo(-1);

        // round towards positive infinity
        assertThat(roundDiv(55555, 1)).isEqualTo(55555);
        assertThat(roundDiv(55555, 10)).isEqualTo(5556);
        assertThat(roundDiv(55555, 100)).isEqualTo(556);
        assertThat(roundDiv(55555, 1000)).isEqualTo(56);
        assertThat(roundDiv(55555, 10000)).isEqualTo(6);
        assertThat(roundDiv(55555, 100000)).isEqualTo(1);

        assertThat(roundDiv(-44445, 1)).isEqualTo(-44445);
        assertThat(roundDiv(-44445, 10)).isEqualTo(-4444);
        assertThat(roundDiv(-44445, 100)).isEqualTo(-444);
        assertThat(roundDiv(-44445, 1000)).isEqualTo(-44);
        assertThat(roundDiv(-44445, 10000)).isEqualTo(-4);
        assertThat(roundDiv(-44445, 100000)).isEqualTo(0);

        assertThatThrownBy(() -> roundDiv(1234, 0))
                .isExactlyInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> roundDiv(1234, -1))
                .isExactlyInstanceOf(IllegalArgumentException.class);
    }
}
