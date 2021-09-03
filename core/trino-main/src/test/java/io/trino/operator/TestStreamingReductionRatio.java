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
package io.trino.operator;

import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.shaded.com.google.common.base.Preconditions.checkArgument;

public class TestStreamingReductionRatio
{
    private static StreamingReductionRatio getAvgOfNumericSeries(int start, int end)
    {
        checkArgument(start <= end);
        StreamingReductionRatio streamingReductionRatio = new StreamingReductionRatio();

        for (int i = start; i < end; ++i) {
            streamingReductionRatio.update(i);
        }

        return streamingReductionRatio;
    }

    @Test
    public void testUpdate()
    {
        // test avg of numeric series
        assertThat(getAvgOfNumericSeries(1, 20).getAverage())
                .isEqualTo(10);

        assertThat(getAvgOfNumericSeries(1, 4000).getAverage())
                .isEqualTo(1500.5);

        // test no avg accounted for taken after 4000 rows (sample size)
        assertThat(getAvgOfNumericSeries(1, 10000).getAverage())
                .isEqualTo(1500.5);
    }
}
