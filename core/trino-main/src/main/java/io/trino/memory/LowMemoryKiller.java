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

package io.trino.memory;

import io.trino.execution.TaskId;
import io.trino.operator.RetryPolicy;
import io.trino.spi.QueryId;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public interface LowMemoryKiller
{
    Optional<KillTarget> chooseQueryToKill(List<QueryMemoryInfo> runningQueries, List<MemoryInfo> nodes);

    class QueryMemoryInfo
    {
        private final QueryId queryId;
        private final long memoryReservation;
        private final RetryPolicy retryPolicy;

        public QueryMemoryInfo(QueryId queryId, long memoryReservation, RetryPolicy retryPolicy)
        {
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.memoryReservation = memoryReservation;
            this.retryPolicy = requireNonNull(retryPolicy, "retryPolicy is null");
        }

        public QueryId getQueryId()
        {
            return queryId;
        }

        public long getMemoryReservation()
        {
            return memoryReservation;
        }

        public RetryPolicy getRetryPolicy()
        {
            return retryPolicy;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("queryId", queryId)
                    .add("memoryReservation", memoryReservation)
                    .add("retryPolicy", retryPolicy)
                    .toString();
        }

        public static class TaskMemoryInfo
        {
            private final TaskId taskId;
            private final long memoryReservation;

            public TaskMemoryInfo(TaskId taskId, long memoryReservation)
            {
                this.taskId = requireNonNull(taskId, "taskId is null");
                this.memoryReservation = memoryReservation;
            }

            public TaskId getTaskId()
            {
                return taskId;
            }

            public long getMemoryReservation()
            {
                return memoryReservation;
            }

            @Override
            public String toString()
            {
                return toStringHelper(this)
                        .add("taskId", taskId)
                        .add("memoryReservation", memoryReservation)
                        .toString();
            }
        }
    }
}
