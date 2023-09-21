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

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;
import io.trino.execution.SplitRunner;
import io.trino.execution.TaskId;

import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.DoubleSupplier;
import java.util.function.Predicate;

public interface TaskExecutor
{
    TaskHandle addTask(
            TaskId taskId,
            DoubleSupplier utilizationSupplier,
            int initialSplitConcurrency,
            Duration splitConcurrencyAdjustFrequency,
            OptionalInt maxDriversPerTask);

    void removeTask(TaskHandle taskHandle);

    List<ListenableFuture<Void>> enqueueSplits(TaskHandle taskHandle, boolean intermediate, List<? extends SplitRunner> taskSplits);

    Set<TaskId> getStuckSplitTaskIds(Duration processingDurationThreshold, Predicate<RunningSplitInfo> filter);

    void start();

    void stop();
}
