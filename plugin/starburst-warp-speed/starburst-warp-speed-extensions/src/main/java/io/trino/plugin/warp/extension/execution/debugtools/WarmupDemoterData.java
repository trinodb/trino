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
package io.trino.plugin.warp.extension.execution.debugtools;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.varada.execution.TaskData;
import io.trino.plugin.varada.execution.debugtools.WarmupDemoterWarmupElementData;
import io.trino.spi.connector.SchemaTableName;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class WarmupDemoterData
        extends TaskData
{
    private final double maxUsageThresholdInPercentage;
    private final double cleanupUsageThresholdInPercentage;
    private final double epsilon;
    private final long maxElementsToDemoteInIteration;
    private final SchemaTableName schemaTableName;
    private final List<WarmupDemoterWarmupElementData> warmupElementsData;
    private final Set<String> filePaths;
    private final int batchSize;
    private final boolean executeDemoter;
    private final boolean modifyConfiguration;
    private final boolean forceExecuteDeadObjects;
    private final boolean resetHighestPriority;
    private final boolean forceDeleteFailedObjects;
    private final WarmupDemoterThreshold warmupDemoterThreshold;

    private final boolean enableDemoteFeature;

    @JsonCreator
    public WarmupDemoterData(@JsonProperty(value = "maxUsageThresholdInPercentage") Double maxUsageThresholdInPercentage,
            @JsonProperty(value = "cleanupUsageThresholdInPercentage") Double cleanupUsageThresholdInPercentage,
            @JsonProperty(value = "batchSize", defaultValue = "-1") Integer batchSize,
            @JsonProperty(value = "schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty(value = "warmupElementsData") List<WarmupDemoterWarmupElementData> warmupElementsData,
            @JsonProperty(value = "filePaths") Set<String> filePaths,
            @JsonProperty(value = "executeDemoter", defaultValue = "true") boolean executeDemoter,
            @JsonProperty(value = "modifyConfiguration", defaultValue = "false") boolean modifyConfiguration,
            @JsonProperty(value = "forceExecuteDeadObjects", defaultValue = "false") boolean forceExecuteDeadObjects,
            @JsonProperty(value = "forceDeleteFailedObjects", defaultValue = "false") boolean forceDeleteFailedObjects,
            @JsonProperty(value = "resetHighestPriority", defaultValue = "false") boolean resetHighestPriority,
            @JsonProperty(value = "warmupDemoterThreshold") WarmupDemoterThreshold warmupDemoterThreshold,
            @JsonProperty(value = "epsilon") Double epsilon,
            @JsonProperty(value = "maxElementsToDemoteInIteration") Long maxElementsToDemoteInIteration,
            @JsonProperty(value = "enableDemoteFeature", defaultValue = "true") Boolean enableDemoteFeature)
    {
        this.maxUsageThresholdInPercentage = maxUsageThresholdInPercentage != null ? maxUsageThresholdInPercentage : -1;
        this.cleanupUsageThresholdInPercentage = cleanupUsageThresholdInPercentage != null ? cleanupUsageThresholdInPercentage : -1;
        this.schemaTableName = schemaTableName;
        this.warmupElementsData = warmupElementsData;
        this.filePaths = filePaths;
        this.batchSize = batchSize != null ? batchSize : -1;
        this.executeDemoter = executeDemoter;
        this.modifyConfiguration = modifyConfiguration;
        this.forceExecuteDeadObjects = forceExecuteDeadObjects;
        this.forceDeleteFailedObjects = forceDeleteFailedObjects;
        this.resetHighestPriority = resetHighestPriority;
        this.warmupDemoterThreshold = warmupDemoterThreshold;
        this.epsilon = epsilon != null ? epsilon : -1;
        this.maxElementsToDemoteInIteration = maxElementsToDemoteInIteration != null ? maxElementsToDemoteInIteration : -1;
        this.enableDemoteFeature = enableDemoteFeature != null ? enableDemoteFeature : true;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    @JsonProperty
    public double getMaxUsageThresholdInPercentage()
    {
        return maxUsageThresholdInPercentage;
    }

    @JsonProperty
    public double getCleanupUsageThresholdInPercentage()
    {
        return cleanupUsageThresholdInPercentage;
    }

    @JsonProperty
    public int getBatchSize()
    {
        return batchSize;
    }

    @JsonProperty
    public boolean isExecuteDemoter()
    {
        return executeDemoter;
    }

    @JsonProperty
    public boolean isModifyConfiguration()
    {
        return modifyConfiguration;
    }

    @JsonProperty
    public boolean isForceExecuteDeadObjects()
    {
        return forceExecuteDeadObjects;
    }

    @JsonProperty
    public boolean isForceDeleteFailedObjects()
    {
        return forceDeleteFailedObjects;
    }

    @JsonProperty
    public boolean isResetHighestPriority()
    {
        return resetHighestPriority;
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty("warmupElementsData")
    public List<WarmupDemoterWarmupElementData> getWarmupElementsData()
    {
        return warmupElementsData;
    }

    @JsonProperty
    public Set<String> getFilePaths()
    {
        return filePaths;
    }

    @JsonProperty("warmupDemoterThreshold")
    public WarmupDemoterThreshold getWarmupDemoterThreshold()
    {
        return warmupDemoterThreshold;
    }

    @Override
    protected String getTaskName()
    {
        return WorkerWarmupDemoterTask.WARMUP_DEMOTER_START_TASK_NAME;
    }

    @JsonProperty
    public double getEpsilon()
    {
        return epsilon;
    }

    @JsonProperty
    public long getMaxElementsToDemoteInIteration()
    {
        return maxElementsToDemoteInIteration;
    }

    @JsonProperty
    public boolean isEnableDemoteFeature()
    {
        return enableDemoteFeature;
    }

    @Override
    public String toString()
    {
        return "WarmupDemoterData{" +
                "maxUsageThresholdInPercentage=" + maxUsageThresholdInPercentage +
                ", cleanupUsageThresholdInPercentage=" + cleanupUsageThresholdInPercentage +
                ", schemaTableName=" + schemaTableName +
                ", warmupElementsData=" + warmupElementsData +
                ", filePaths=" + filePaths +
                ", batchSize=" + batchSize +
                ", executeDemoter=" + executeDemoter +
                ", modifyConfiguration=" + modifyConfiguration +
                ", forceExecuteDeadObjects=" + forceExecuteDeadObjects +
                ", forceDeleteFailedObjects=" + forceDeleteFailedObjects +
                ", resetHighestPriority=" + resetHighestPriority +
                ", warmupDemoterThreshold=" + warmupDemoterThreshold +
                ", epsilon=" + epsilon +
                ", maxElementsToDemoteInIteration=" + maxElementsToDemoteInIteration +
                ", enableDemoteFeature=" + enableDemoteFeature +
                '}';
    }

    public static class Builder
    {
        private double maxUsageThresholdInPercentage = -1;
        private double cleanupUsageThresholdInPercentage = -1;
        private double epsilon = -1;
        private long maxElementsToDemoteInIteration = -1;
        private SchemaTableName schemaTableName;
        private List<WarmupDemoterWarmupElementData> warmupElementsData = Collections.emptyList();
        private Set<String> filePaths = Collections.emptySet();
        private int batchSize = -1;
        private boolean executeDemoter;
        private boolean modifyConfiguration = true;
        private boolean forceExecuteDeadObjects;
        private boolean forceDeleteFailedObjects;
        private boolean resetHighestPriority;
        private WarmupDemoterThreshold warmupDemoterThreshold;
        private boolean enableDemoteFeature = true;

        private Builder()
        {
        }

        public Builder maxUsageThresholdInPercentage(double maxUsageThresholdInPercentage)
        {
            this.maxUsageThresholdInPercentage = maxUsageThresholdInPercentage;
            return this;
        }

        public Builder cleanupUsageThresholdInPercentage(double cleanupUsageThresholdInPercentage)
        {
            this.cleanupUsageThresholdInPercentage = cleanupUsageThresholdInPercentage;
            return this;
        }

        public Builder epsilon(double epsilon)
        {
            this.epsilon = epsilon;
            return this;
        }

        public Builder maxElementsToDemoteInIteration(long maxElementsToDemoteInIteration)
        {
            this.maxElementsToDemoteInIteration = maxElementsToDemoteInIteration;
            return this;
        }

        public Builder schemaTableName(SchemaTableName schemaTableName)
        {
            this.schemaTableName = schemaTableName;
            return this;
        }

        public Builder warmupElementsData(List<WarmupDemoterWarmupElementData> warmupElementsData)
        {
            this.warmupElementsData = warmupElementsData;
            return this;
        }

        public Builder filePaths(Set<String> filePaths)
        {
            this.filePaths = filePaths;
            return this;
        }

        public Builder batchSize(int batchSize)
        {
            this.batchSize = batchSize;
            return this;
        }

        public Builder executeDemoter(boolean executeDemoter)
        {
            this.executeDemoter = executeDemoter;
            return this;
        }

        public Builder modifyConfiguration(boolean modifyConfiguration)
        {
            this.modifyConfiguration = modifyConfiguration;
            return this;
        }

        public Builder forceExecuteDeadObjects(boolean forceExecuteDeadObjects)
        {
            this.forceExecuteDeadObjects = forceExecuteDeadObjects;
            return this;
        }

        public Builder forceDeleteFailedObjects(boolean forceDeleteFailedObjects)
        {
            this.forceDeleteFailedObjects = forceDeleteFailedObjects;
            return this;
        }

        public Builder resetHighestPriority(boolean resetHighestPriority)
        {
            this.resetHighestPriority = resetHighestPriority;
            return this;
        }

        public Builder warmupDemoterThreshold(WarmupDemoterThreshold warmupDemoterThreshold)
        {
            this.warmupDemoterThreshold = warmupDemoterThreshold;
            return this;
        }

        public Builder enableDemoteFeature(boolean enableDemoteFeature)
        {
            this.enableDemoteFeature = enableDemoteFeature;
            return this;
        }

        public WarmupDemoterData build()
        {
            return new WarmupDemoterData(maxUsageThresholdInPercentage,
                    cleanupUsageThresholdInPercentage,
                    batchSize,
                    schemaTableName,
                    warmupElementsData,
                    filePaths,
                    executeDemoter,
                    modifyConfiguration,
                    forceExecuteDeadObjects,
                    forceDeleteFailedObjects,
                    resetHighestPriority,
                    warmupDemoterThreshold,
                    epsilon,
                    maxElementsToDemoteInIteration,
                    enableDemoteFeature);
        }
    }
}
