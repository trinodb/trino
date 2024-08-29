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
package io.trino.plugin.tpch;

import io.airlift.slice.Slice;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.Estimate;

import java.util.Optional;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.withinPercentage;

class EstimateAssertion
{
    private final double tolerance;

    public EstimateAssertion(double tolerance)
    {
        this.tolerance = tolerance;
    }

    public void assertClose(Estimate actual, Estimate expected, String comparedValue)
    {
        assertClose(toOptional(actual), toOptional(expected), comparedValue);
    }

    private Optional<Double> toOptional(Estimate estimate)
    {
        return estimate.isUnknown() ? Optional.empty() : Optional.of(estimate.getValue());
    }

    public void assertClose(Optional<?> actual, Optional<?> expected, String comparedValue)
    {
        if (actual.isPresent() != expected.isPresent()) {
            // Trigger exception message that includes compared values
            assertThat(actual)
                    .describedAs(comparedValue)
                    .isEqualTo(expected);
        }
        if (actual.isPresent()) {
            Object actualValue = actual.get();
            Object expectedValue = expected.get();
            assertClose(actualValue, expectedValue, comparedValue);
        }
    }

    private void assertClose(Object actual, Object expected, String comparedValue)
    {
        if (actual instanceof Slice actualSlice) {
            assertThat(actual.getClass())
                    .describedAs(comparedValue)
                    .isEqualTo(expected.getClass());
            assertThat(actualSlice.toStringUtf8())
                    .isEqualTo(((Slice) expected).toStringUtf8());
        }
        else if (actual instanceof DoubleRange actualRange) {
            DoubleRange expectedRange = (DoubleRange) expected;
            assertClose(actualRange.getMin(), expectedRange.getMin(), comparedValue);
            assertClose(actualRange.getMax(), expectedRange.getMax(), comparedValue);
        }
        else {
            double actualDouble = toDouble(actual);
            double expectedDouble = toDouble(expected);
            assertThat(actualDouble)
                    .isCloseTo(expectedDouble, withinPercentage(tolerance * 100));
        }
    }

    private double toDouble(Object object)
    {
        if (object instanceof Number) {
            return ((Number) object).doubleValue();
        }
        throw new UnsupportedOperationException(format("Can't compare with tolerance objects of class %s. Use assertEquals.", object.getClass()));
    }
}
