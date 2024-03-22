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

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record CommitInfoEntry(
        long version,
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
        Optional<Boolean> isBlindAppend)
{
    public CommitInfoEntry
    {
        requireNonNull(isBlindAppend, "isBlindAppend is null");
    }

    public CommitInfoEntry withVersion(long version)
    {
        return new CommitInfoEntry(version, timestamp, userId, userName, operation, operationParameters, job, notebook, clusterId, readVersion, isolationLevel, isBlindAppend);
    }

    public record Job(String jobId, String jobName, String runId, String jobOwnerId, String triggerType) {}

    public record Notebook(String notebookId) {}
}
