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

import com.google.common.collect.ImmutableMap;
import com.google.inject.BindingAnnotation;
import io.trino.execution.TaskId;
import io.trino.execution.TaskInfo;
import io.trino.operator.RetryPolicy;
import io.trino.spi.QueryId;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

public interface LowMemoryKiller
{
    Optional<KillTarget> chooseTargetToKill(List<RunningQueryInfo> runningQueries, List<MemoryInfo> nodes);

    class RunningQueryInfo
    {
        private final QueryId queryId;
        private final long memoryReservation;
        private final Map<TaskId, TaskInfo> taskInfos;
        private final RetryPolicy retryPolicy;

        public RunningQueryInfo(
                QueryId queryId,
                long memoryReservation,
                Map<TaskId, TaskInfo> taskInfos,
                RetryPolicy retryPolicy)
        {
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.memoryReservation = memoryReservation;
            requireNonNull(taskInfos, "taskInfos is null");
            this.taskInfos = ImmutableMap.copyOf(taskInfos);
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

        public Map<TaskId, TaskInfo> getTaskInfos()
        {
            return taskInfos;
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
                    .add("taskStats", taskInfos)
                    .add("retryPolicy", retryPolicy)
                    .toString();
        }
    }

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    @interface ForQueryLowMemoryKiller {}

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    @interface ForTaskLowMemoryKiller {}
}
