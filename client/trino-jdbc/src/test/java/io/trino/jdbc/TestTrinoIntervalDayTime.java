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
package io.trino.jdbc;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTrinoIntervalDayTime
{
    @Test
    public void testMillisecondConstructorPreservesUnit()
    {
        // the single-argument constructor is milliseconds — the unit it has always used
        assertThat(new TrinoIntervalDayTime(1000).getMilliSeconds()).isEqualTo(1000L);
        assertThat(new TrinoIntervalDayTime(-1500).getMilliSeconds()).isEqualTo(-1500L);
    }

    @Test
    public void testMillisecondConstructorRejectsOverflow()
    {
        // the microsecond-backed representation cannot hold the full legacy millisecond range, so a value
        // that would overflow is rejected rather than silently wrapped to a different interval
        assertThatThrownBy(() -> new TrinoIntervalDayTime(Long.MAX_VALUE))
                .isInstanceOf(ArithmeticException.class);
    }

    @Test
    public void testExposesPreciseComponents()
    {
        TrinoIntervalDayTime interval = new TrinoIntervalDayTime(1_500_000, 250_000, 9);
        assertThat(interval.getMicroSeconds()).isEqualTo(1_500_000L);
        assertThat(interval.getPicosOfMicro()).isEqualTo(250_000);
        assertThat(interval.getMilliSeconds()).isEqualTo(1_500L);
    }

    @Test
    public void testConstructorValidatesInvariants()
    {
        assertThatThrownBy(() -> new TrinoIntervalDayTime(0, 1_000_000, 6))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new TrinoIntervalDayTime(0, -1, 6))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new TrinoIntervalDayTime(0, 0, 13))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
