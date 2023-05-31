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

import io.trino.execution.ScheduledSplit;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.Optional;
import java.util.OptionalInt;

public interface SplitDriverFactory
{
    int getPipelineId();

    boolean isInputDriver();

    boolean isOutputDriver();

    OptionalInt getDriverInstances();

    Driver createDriver(DriverContext driverContext, Optional<ScheduledSplit> split);

    default Driver createDriver(DriverContext driverContext)
    {
        return createDriver(driverContext, Optional.empty());
    }

    void noMoreDrivers();

    boolean isNoMoreDrivers();

    void localPlannerComplete();

    /**
     * return the sourceId of this DriverFactory.
     * A DriverFactory doesn't always have source node.
     * For example, ValuesNode is not a source node.
     */
    Optional<PlanNodeId> getSourceId();
}
