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
package io.trino.cost;

import io.trino.Session;
import org.junit.jupiter.api.Test;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.SystemSessionProperties.LOW_CONFIDENCE_COST_MARGIN;
import static io.trino.cost.EstimateConfidence.FACT;
import static io.trino.cost.EstimateConfidence.HIGH;
import static io.trino.cost.EstimateConfidence.LOW;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestCostComparator
{
    @Test
    public void testCpuWeight()
    {
        new CostComparisonAssertion(1.0, 0.0, 0.0)
                .smaller(200, 200, 200)
                .larger(1000, 100, 100)
                .assertCompare();
    }

    @Test
    public void testMemoryWeight()
    {
        new CostComparisonAssertion(0.0, 1.0, 0.0)
                .smaller(200, 200, 200)
                .larger(100, 1000, 100)
                .assertCompare();
    }

    @Test
    public void testNetworkWeight()
    {
        new CostComparisonAssertion(0.0, 0.0, 1.0)
                .smaller(200, 200, 200)
                .larger(100, 100, 1000)
                .assertCompare();
    }

    @Test
    public void testAllWeights()
    {
        new CostComparisonAssertion(1.0, 1.0, 1.0)
                .smaller(333, 333, 333)
                .larger(200, 300, 500)
                .assertCompare();

        new CostComparisonAssertion(1.0, 1000.0, 1.0)
                .smaller(300, 299, 300)
                .larger(100, 300, 100)
                .assertCompare();
    }

    @Test
    public void testLowConfidenceCostMargin()
    {
        CostComparator costComparator = new CostComparator(1.0, 1.0, 1.0);
        PlanCostEstimate cheapButGuessed = new PlanCostEstimate(100, 100, 100, 100).withConfidence(LOW);
        PlanCostEstimate dearerButReported = new PlanCostEstimate(150, 150, 150, 150).withConfidence(HIGH);

        // without a margin the cheaper cost wins regardless of where it came from
        Session noMargin = testSessionBuilder()
                .setSystemProperty(LOW_CONFIDENCE_COST_MARGIN, "1.0")
                .build();
        assertThat(costComparator.compare(noMargin, cheapButGuessed, dearerButReported)).isLessThan(0);

        // a margin of 2 means the guessed plan has to be less than half the cost to be chosen
        Session withMargin = testSessionBuilder()
                .setSystemProperty(LOW_CONFIDENCE_COST_MARGIN, "2.0")
                .build();
        assertThat(costComparator.compare(withMargin, cheapButGuessed, dearerButReported)).isGreaterThan(0);
        assertThat(costComparator.compare(withMargin, dearerButReported, cheapButGuessed)).isLessThan(0);

        // the margin only applies between differing confidences
        PlanCostEstimate alsoGuessed = new PlanCostEstimate(150, 150, 150, 150).withConfidence(LOW);
        assertThat(costComparator.compare(withMargin, cheapButGuessed, alsoGuessed)).isLessThan(0);

        // estimates that were not guessed are all trusted alike: the margin never separates
        // statistics the connector reported from exact figures
        PlanCostEstimate cheapButReported = new PlanCostEstimate(100, 100, 100, 100).withConfidence(HIGH);
        PlanCostEstimate dearerButExact = new PlanCostEstimate(150, 150, 150, 150).withConfidence(FACT);
        assertThat(costComparator.compare(withMargin, cheapButReported, dearerButExact)).isLessThan(0);
        assertThat(costComparator.compare(withMargin, dearerButExact, cheapButReported)).isGreaterThan(0);
    }

    @Test
    public void testUnknownCost()
    {
        CostComparator costComparator = new CostComparator(1.0, 1.0, 1.0);
        Session session = testSessionBuilder().build();
        assertThatThrownBy(() -> costComparator.compare(session, PlanCostEstimate.zero(), PlanCostEstimate.unknown()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("cannot compare unknown costs");
        assertThatThrownBy(() -> costComparator.compare(session, PlanCostEstimate.unknown(), PlanCostEstimate.zero()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("cannot compare unknown costs");
        assertThatThrownBy(() -> costComparator.compare(session, PlanCostEstimate.unknown(), PlanCostEstimate.unknown()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("cannot compare unknown costs");
    }

    private static class CostComparisonAssertion
    {
        private final CostComparator costComparator;
        private final Session session = testSessionBuilder().build();

        private PlanCostEstimate smaller;
        private PlanCostEstimate larger;

        public CostComparisonAssertion(double cpuWeight, double memoryWeight, double networkWeight)
        {
            costComparator = new CostComparator(cpuWeight, memoryWeight, networkWeight);
        }

        public void assertCompare()
        {
            checkState(smaller != null, "smaller not set");
            checkState(larger != null, "larger not set");
            assertThat(costComparator.compare(session, smaller, larger) < 0)
                    .describedAs("smaller < larger is false")
                    .isTrue();
            assertThat(costComparator.compare(session, larger, smaller) > 0)
                    .describedAs("larger > smaller is false")
                    .isTrue();
        }

        public CostComparisonAssertion smaller(double cpu, double memory, double network)
        {
            checkState(smaller == null, "already set");
            smaller = new PlanCostEstimate(cpu, memory, 0, network);
            return this;
        }

        public CostComparisonAssertion larger(double cpu, double memory, double network)
        {
            checkState(larger == null, "already set");
            larger = new PlanCostEstimate(cpu, memory, 0, network);
            return this;
        }
    }
}
