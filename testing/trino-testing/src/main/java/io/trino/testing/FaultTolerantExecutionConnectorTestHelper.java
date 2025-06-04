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
package io.trino.testing;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public final class FaultTolerantExecutionConnectorTestHelper
{
    private FaultTolerantExecutionConnectorTestHelper() {}

    public static Map<String, String> getExtraProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("retry-policy", "TASK")
                .put("retry-initial-delay", "50ms")
                .put("retry-max-delay", "100ms")
                .put("fault-tolerant-execution-min-partition-count", "4")
                .put("fault-tolerant-execution-max-partition-count", "5")
                .put("fault-tolerant-execution-min-partition-count-for-write", "4")
                .put("fault-tolerant-execution-arbitrary-distribution-compute-task-target-size-min", "5MB")
                .put("fault-tolerant-execution-arbitrary-distribution-compute-task-target-size-max", "10MB")
                .put("fault-tolerant-execution-arbitrary-distribution-write-task-target-size-min", "10MB")
                .put("fault-tolerant-execution-arbitrary-distribution-write-task-target-size-max", "20MB")
                .put("fault-tolerant-execution-hash-distribution-compute-task-target-size", "5MB")
                .put("fault-tolerant-execution-hash-distribution-write-task-target-size", "10MB")
                .put("fault-tolerant-execution-standard-split-size", "2.5MB")
                .put("fault-tolerant-execution-hash-distribution-compute-task-to-node-min-ratio", "0.0")
                .put("fault-tolerant-execution-hash-distribution-write-task-to-node-min-ratio", "0.0")
                // test task compression aggressively
                .put("fault-tolerant-execution-task-descriptor-storage-high-water-mark", "1kB")
                .put("fault-tolerant-execution-task-descriptor-storage-low-water-mark", "200B")
                // to trigger spilling
                .put("exchange.deduplication-buffer-size", "1kB")
                .put("fault-tolerant-execution-task-memory", "1GB")

                // enable exchange compression to follow production deployment recommendations
                .put("exchange.compression-codec", "LZ4")
                .put("max-tasks-waiting-for-execution-per-query", "2")
                .put("max-tasks-waiting-for-node-per-query", "2")
                .put("query.schedule-split-batch-size", "2")
                .buildOrThrow();
    }

    public static Map<String, String> enforceRuntimeAdaptivePartitioningProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("fault-tolerant-execution-adaptive-query-planning-enabled", "true")
                .put("fault-tolerant-execution-runtime-adaptive-partitioning-enabled", "true")
                .put("fault-tolerant-execution-runtime-adaptive-partitioning-partition-count", "40")
                // to ensure runtime adaptive partitioning is triggered
                .put("fault-tolerant-execution-runtime-adaptive-partitioning-max-task-size", "1B")
                .buildOrThrow();
    }
}
