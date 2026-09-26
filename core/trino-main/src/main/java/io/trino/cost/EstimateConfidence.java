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

/**
 * How much the engine trusts a {@link PlanNodeStatsEstimate}.
 * <p>
 * Confidence is derived by the engine rather than reported by a connector. An estimate is
 * degraded to {@link #LOW} whenever a heuristic constant or an unknown input contributed to it,
 * for example a default filter selectivity or a join key with no distinct value count. Because
 * confidence propagates along with the estimate it describes, the value at a plan node reflects
 * the least trustworthy estimate anywhere beneath it.
 * <p>
 * Constants are declared from least to most trustworthy, so {@link #compareTo} orders them by
 * confidence.
 */
public enum EstimateConfidence
{
    /**
     * A heuristic constant or an unknown input contributed to the estimate, so it may be wrong
     * by an unbounded factor.
     */
    LOW,
    /**
     * The estimate was derived entirely from statistics reported by the connector.
     */
    HIGH,
    /**
     * The estimate is exact, for example the row count of a values node.
     */
    FACT;

    /**
     * Returns the less trustworthy of the two levels. Combining estimates cannot produce
     * something more trustworthy than the weakest input.
     */
    public static EstimateConfidence min(EstimateConfidence left, EstimateConfidence right)
    {
        return left.compareTo(right) <= 0 ? left : right;
    }
}
