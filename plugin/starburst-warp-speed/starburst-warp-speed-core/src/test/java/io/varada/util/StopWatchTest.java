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
package io.varada.util;

import io.varada.tools.util.StopWatch;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class StopWatchTest
{
    @Test
    public void test()
    {
        StopWatch stopWatch = new StopWatch();
        assertThat(stopWatch.getNanoTime()).isEqualTo(0);
        stopWatch.start();
        assertThat(stopWatch.getNanoTime()).isGreaterThan(0);
        assertThat(stopWatch.isStarted()).isTrue();
        stopWatch.stop();
        assertThat(stopWatch.getNanoTime()).isGreaterThan(0);
        assertThat(stopWatch.isStarted()).isFalse();
        stopWatch.reset();
        assertThat(stopWatch.getNanoTime()).isEqualTo(0);
    }
}
