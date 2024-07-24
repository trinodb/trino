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

import io.trino.memory.context.MemoryTrackingContext;
import io.trino.metadata.Split;
import io.trino.sql.planner.plan.PlanNodeId;

public interface WorkProcessorSourceOperatorFactory
{
    int getOperatorId();

    PlanNodeId getSourceId();

    PlanNodeId getPlanNodeId();

    String getOperatorType();

    WorkProcessorSourceOperator create(
            OperatorContext operatorContext,
            MemoryTrackingContext memoryTrackingContext,
            DriverYieldSignal yieldSignal,
            WorkProcessor<Split> splits);
}
