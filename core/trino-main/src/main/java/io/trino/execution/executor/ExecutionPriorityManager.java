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
package io.trino.execution.executor;

import io.trino.Session;
import io.trino.execution.TaskManagerConfig;
import org.assertj.core.util.VisibleForTesting;

import static io.trino.SystemSessionProperties.getQueryExecutionPriority;

public class ExecutionPriorityManager
{
    private final ExecutionPriority lowPriority;

    public ExecutionPriorityManager(TaskManagerConfig taskManagerConfig)
    {
        this(taskManagerConfig.getLowQueryPriorityResourcePercentage());
    }

    @VisibleForTesting
    public ExecutionPriorityManager(double lowQueryPriorityResourcePercentage)
    {
        this.lowPriority = ExecutionPriority.fromResourcePercentage(lowQueryPriorityResourcePercentage);
    }

    public ExecutionPriority getPriority(Session session)
    {
        QueryExecutionPriority queryExecutionPriority = getQueryExecutionPriority(session);
        return switch (queryExecutionPriority) {
            case NORMAL -> ExecutionPriority.NORMAL;
            case LOW -> lowPriority;
        };
    }
}
