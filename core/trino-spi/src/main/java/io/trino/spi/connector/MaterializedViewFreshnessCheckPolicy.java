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

package io.trino.spi.connector;

import java.time.Instant;

/**
 * Policy for checking materialized view freshness, guiding a connector on whether it can
 * optimize freshness checks.
 */
public sealed interface MaterializedViewFreshnessCheckPolicy
        permits MaterializedViewFreshnessCheckPolicy.Exact, MaterializedViewFreshnessCheckPolicy.ConsiderGracePeriod
{
    /**
     * Policy requiring exact freshness check.
     */
    record Exact()
            implements MaterializedViewFreshnessCheckPolicy {}

    /**
     * Policy that allows optimizing freshness checks by considering the materialized view's
     * grace period. A connector may skip expensive operations (e.g., metastore calls to check
     * base table snapshots) if it can determine the MV is fresh enough based on the
     * referenceTime and the grace period configured in the materialized view definition.
     *
     * @param referenceTime The point in time against which freshness is being evaluated
     *                      (typically the query start time)
     */
    record ConsiderGracePeriod(Instant referenceTime)
            implements MaterializedViewFreshnessCheckPolicy {}
}
