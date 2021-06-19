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

import static io.trino.spi.type.Timestamps.roundDiv;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTimestamps
{
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
