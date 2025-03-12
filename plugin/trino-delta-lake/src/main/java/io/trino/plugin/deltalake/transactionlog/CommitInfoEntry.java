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
package io.trino.plugin.deltalake.transactionlog;

import io.airlift.slice.SizeOf;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public record CommitInfoEntry(
        long version,
        OptionalLong inCommitTimestamp,
        long timestamp,
        String userId,
        String userName,
        String operation,
        Map<String, String> operationParameters,
        Job job,
        Notebook notebook,
        String clusterId,
        long readVersion,
        String isolationLevel,
        Optional<Boolean> isBlindAppend,
        Map<String, String> operationMetrics)
{
    private static final int INSTANCE_SIZE = instanceSize(CommitInfoEntry.class);

    public CommitInfoEntry
    {
        requireNonNull(isBlindAppend, "isBlindAppend is null");
    }

    public CommitInfoEntry withVersion(long version)
    {
        return new CommitInfoEntry(version, inCommitTimestamp, timestamp, userId, userName, operation, operationParameters, job, notebook, clusterId, readVersion, isolationLevel, isBlindAppend, operationMetrics);
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + SIZE_OF_LONG
                + sizeOf(inCommitTimestamp)
                + SIZE_OF_LONG
                + estimatedSizeOf(userId)
                + estimatedSizeOf(userName)
                + estimatedSizeOf(operation)
                + estimatedSizeOf(operationParameters, SizeOf::estimatedSizeOf, SizeOf::estimatedSizeOf)
                + (job == null ? 0 : job.getRetainedSizeInBytes())
                + (notebook == null ? 0 : notebook.getRetainedSizeInBytes())
                + estimatedSizeOf(clusterId)
                + SIZE_OF_LONG
                + estimatedSizeOf(isolationLevel)
                + sizeOf(isBlindAppend, SizeOf::sizeOf)
                + estimatedSizeOf(operationMetrics, SizeOf::estimatedSizeOf, SizeOf::estimatedSizeOf);
    }

    public record Job(String jobId, String jobName, String runId, String jobOwnerId, String triggerType)
    {
        private static final int INSTANCE_SIZE = instanceSize(Job.class);

        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE
                    + estimatedSizeOf(jobId)
                    + estimatedSizeOf(jobName)
                    + estimatedSizeOf(runId)
                    + estimatedSizeOf(jobOwnerId)
                    + estimatedSizeOf(triggerType);
        }
    }

    public record Notebook(String notebookId)
    {
        private static final int INSTANCE_SIZE = instanceSize(Notebook.class);

        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE + estimatedSizeOf(notebookId);
        }
    }
}
