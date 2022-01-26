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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Objects;

import static java.lang.String.format;

public class CommitInfoEntry
{
    private final long version;
    private final long timestamp;
    private final String userId;
    private final String userName;
    private final String operation;
    private final Map<String, String> operationParameters;
    private final Job job;
    private final Notebook notebook;
    private final String clusterId;
    private final long readVersion;
    private final String isolationLevel;
    private final boolean isBlindAppend;

    @JsonCreator
    public CommitInfoEntry(
            @JsonProperty("version") long version,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("userId") String userId,
            @JsonProperty("userName") String userName,
            @JsonProperty("operation") String operation,
            @JsonProperty("operationParameters") Map<String, String> operationParameters,
            @JsonProperty("job") Job job,
            @JsonProperty("notebook") Notebook notebook,
            @JsonProperty("clusterId") String clusterId,
            @JsonProperty("readVersion") long readVersion,
            @JsonProperty("isolationLevel") String isolationLevel,
            @JsonProperty("isBlindAppend") boolean isBlindAppend)
    {
        this.version = version;
        this.timestamp = timestamp;
        this.userId = userId;
        this.userName = userName;
        this.operation = operation;
        this.operationParameters = operationParameters;
        this.job = job;
        this.notebook = notebook;
        this.clusterId = clusterId;
        this.readVersion = readVersion;
        this.isolationLevel = isolationLevel;
        this.isBlindAppend = isBlindAppend;
    }

    @JsonProperty
    public long getVersion()
    {
        return version;
    }

    @JsonProperty
    public long getTimestamp()
    {
        return timestamp;
    }

    @JsonProperty
    public String getUserId()
    {
        return userId;
    }

    @JsonProperty
    public String getUserName()
    {
        return userName;
    }

    @JsonProperty
    public String getOperation()
    {
        return operation;
    }

    @JsonProperty
    public Map<String, String> getOperationParameters()
    {
        return operationParameters;
    }

    @JsonProperty
    public Job getJob()
    {
        return job;
    }

    @JsonProperty
    public Notebook getNotebook()
    {
        return notebook;
    }

    @JsonProperty
    public String getClusterId()
    {
        return clusterId;
    }

    @JsonProperty
    public long getReadVersion()
    {
        return readVersion;
    }

    @JsonProperty
    public String getIsolationLevel()
    {
        return isolationLevel;
    }

    @JsonProperty
    public boolean isBlindAppend()
    {
        return isBlindAppend;
    }

    @Override
    public String toString()
    {
        return format("CommitInfoEntry{version=%d, timestamp=%d, userId=%s, userName=%s, operation=%s, operationParameters=%s, " +
                        "job=%s, notebook=%s, clusterId=%s, readVersion=%d, isolationLevel=%s, isBlindAppend=%b}",
                version, timestamp, userId, userName, operation, operationParameters,
                job, notebook, clusterId, readVersion, isolationLevel, isBlindAppend);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CommitInfoEntry other = (CommitInfoEntry) o;
        return this.version == other.version &&
                this.timestamp == other.timestamp &&
                Objects.equals(this.userId, other.userId) &&
                Objects.equals(this.userName, other.userName) &&
                Objects.equals(this.operation, other.operation) &&
                Objects.equals(this.operationParameters, other.operationParameters) &&
                Objects.equals(this.job, other.job) &&
                Objects.equals(this.notebook, other.notebook) &&
                Objects.equals(this.clusterId, other.clusterId) &&
                this.readVersion == other.readVersion &&
                Objects.equals(this.isolationLevel, other.isolationLevel) &&
                this.isBlindAppend == other.isBlindAppend;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(version, timestamp, userId, userName, operation, operationParameters, job, notebook, clusterId, readVersion, isolationLevel, isBlindAppend);
    }

    public static class Job
    {
        private final String jobId;
        private final String jobName;
        private final String runId;
        private final String jobOwnerId;
        private final String triggerType;

        @JsonCreator
        public Job(
                @JsonProperty("jobId") String jobId,
                @JsonProperty("jobName") String jobName,
                @JsonProperty("runId") String runId,
                @JsonProperty("jobOwnerId") String jobOwnerId,
                @JsonProperty("triggerType") String triggerType)
        {
            this.jobId = jobId;
            this.jobName = jobName;
            this.runId = runId;
            this.jobOwnerId = jobOwnerId;
            this.triggerType = triggerType;
        }

        @JsonProperty
        public String getJobId()
        {
            return jobId;
        }

        @JsonProperty
        public String getJobName()
        {
            return jobName;
        }

        @JsonProperty
        public String getRunId()
        {
            return runId;
        }

        @JsonProperty
        public String getJobOwnerId()
        {
            return jobOwnerId;
        }

        @JsonProperty
        public String getTriggerType()
        {
            return triggerType;
        }

        @Override
        public String toString()
        {
            return format("CommitInfoEntry.Job{jobId=%s, jobName=%s, runId=%s, jobOwnerId=%s, triggerType=%s}",
                    jobId, jobName, runId, jobOwnerId, triggerType);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Job other = (Job) o;
            return Objects.equals(this.jobId, other.jobId) &&
                    Objects.equals(this.jobName, other.jobName) &&
                    Objects.equals(this.runId, other.runId) &&
                    Objects.equals(this.jobOwnerId, other.jobOwnerId) &&
                    Objects.equals(this.triggerType, other.triggerType);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(jobId, jobName, runId, jobOwnerId, triggerType);
        }
    }

    public static class Notebook
    {
        private final String notebookId;

        @JsonCreator
        public Notebook(@JsonProperty("notebookId") String notebookId)
        {
            this.notebookId = notebookId;
        }

        @JsonProperty
        public String getNotebookId()
        {
            return notebookId;
        }

        @Override
        public String toString()
        {
            return format("CommitInfoEntry.Notebook{notebookId=%s}", notebookId);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Notebook other = (Notebook) o;
            return Objects.equals(this.notebookId, other.notebookId);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(notebookId);
        }
    }
}
