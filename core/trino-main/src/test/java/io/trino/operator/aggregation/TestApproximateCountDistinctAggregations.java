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
package io.trino.operator.aggregation;

import org.testng.annotations.Test;

import static io.trino.operator.aggregation.ApproximateCountDistinctAggregation.standardErrorToBuckets;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

public class TestApproximateCountDistinctAggregations
{
    @Test
    public void testStandardErrorToBuckets()
    {
        assertThat(standardErrorToBuckets(0.0326)).isEqualTo(1024);
        assertThat(standardErrorToBuckets(0.0325)).isEqualTo(1024);
        assertThat(standardErrorToBuckets(0.0324)).isEqualTo(2048);
        assertThat(standardErrorToBuckets(0.0231)).isEqualTo(2048);
        assertThat(standardErrorToBuckets(0.0230)).isEqualTo(2048);
        assertThat(standardErrorToBuckets(0.0229)).isEqualTo(4096);
        assertThat(standardErrorToBuckets(0.0164)).isEqualTo(4096);
        assertThat(standardErrorToBuckets(0.0163)).isEqualTo(4096);
        assertThat(standardErrorToBuckets(0.0162)).isEqualTo(8192);
        assertThat(standardErrorToBuckets(0.0116)).isEqualTo(8192);
        assertThat(standardErrorToBuckets(0.0115)).isEqualTo(8192);
        assertThat(standardErrorToBuckets(0.0114)).isEqualTo(16384);
        assertThat(standardErrorToBuckets(0.008126)).isEqualTo(16384);
        assertThat(standardErrorToBuckets(0.008125)).isEqualTo(16384);
        assertThat(standardErrorToBuckets(0.008124)).isEqualTo(32768);
        assertThat(standardErrorToBuckets(0.00576)).isEqualTo(32768);
        assertThat(standardErrorToBuckets(0.00575)).isEqualTo(32768);
        assertThat(standardErrorToBuckets(0.00574)).isEqualTo(65536);
        assertThat(standardErrorToBuckets(0.0040626)).isEqualTo(65536);
        assertThat(standardErrorToBuckets(0.0040625)).isEqualTo(65536);
    }

    @Test
    public void testStandardErrorToBucketsBounds()
    {
        // Lower bound
        assertTrinoExceptionThrownBy(() -> standardErrorToBuckets(0.0040624))
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);

        // Upper bound
        assertTrinoExceptionThrownBy(() -> standardErrorToBuckets(0.26001))
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
    }
}
