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
package io.trino.plugin.varada.api.warmup;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.varada.api.warmup.column.VaradaColumnData;

import java.time.Duration;
import java.util.Objects;

public class WarmupColRuleUsageData
        extends WarmupColRuleData
{
    private final long usedStorageKB;

    @JsonCreator
    public WarmupColRuleUsageData(@JsonProperty("id") int id,
            @JsonProperty("schema") String schema,
            @JsonProperty("table") String table,
            @JsonProperty("column") VaradaColumnData column,
            @JsonProperty("warmUpType") WarmUpType warmUpType,
            @JsonProperty("priority") double priority,
            @JsonProperty("ttl") Duration ttl,
            @JsonProperty("predicates") ImmutableSet<WarmupPredicateRule> predicates,
            @JsonProperty("usedStorageKB") long usedStorageKB)
    {
        super(id, schema, table, column, warmUpType, priority, ttl, predicates);
        this.usedStorageKB = usedStorageKB;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builder(WarmupColRuleUsageData warmupColRuleUsageData)
    {
        return new Builder().id(warmupColRuleUsageData.getId())
                .schema(warmupColRuleUsageData.getSchema())
                .table(warmupColRuleUsageData.getTable())
                .column(warmupColRuleUsageData.getColumn())
                .predicates(warmupColRuleUsageData.getPredicates())
                .warmUpType(warmupColRuleUsageData.getWarmUpType())
                .priority(warmupColRuleUsageData.getPriority())
                .ttl(warmupColRuleUsageData.getTtl())
                .usedStorageKB(warmupColRuleUsageData.getUsedStorageKB());
    }

    @JsonProperty
    public long getUsedStorageKB()
    {
        return usedStorageKB;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof WarmupColRuleUsageData that)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        return getUsedStorageKB() == that.getUsedStorageKB();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(), getUsedStorageKB());
    }

    @Override
    public String toString()
    {
        return "WarmupColRuleUsageData{" +
                "usedStorageKB=" + usedStorageKB +
                "} " + super.toString();
    }

    public static class Builder
    {
        private int id;
        private String schema;
        private String table;
        private VaradaColumnData column;
        private ImmutableSet<WarmupPredicateRule> predicates;
        private WarmUpType warmUpType;
        private double priority;
        private Duration ttl;
        private long usedStorageKB;

        private Builder()
        {
        }

        public Builder id(int id)
        {
            this.id = id;
            return this;
        }

        public Builder schema(String schema)
        {
            this.schema = schema;
            return this;
        }

        public Builder table(String table)
        {
            this.table = table;
            return this;
        }

        public Builder column(VaradaColumnData column)
        {
            this.column = column;
            return this;
        }

        public Builder predicates(ImmutableSet<WarmupPredicateRule> predicates)
        {
            this.predicates = predicates;
            return this;
        }

        public Builder warmUpType(WarmUpType warmUpType)
        {
            this.warmUpType = warmUpType;
            return this;
        }

        public Builder priority(double priority)
        {
            this.priority = priority;
            return this;
        }

        public Builder ttl(Duration ttl)
        {
            this.ttl = ttl;
            return this;
        }

        public Builder usedStorageKB(long usedStorageKB)
        {
            this.usedStorageKB = usedStorageKB;
            return this;
        }

        public Builder increaseStorageKB(long usedStorageKB)
        {
            this.usedStorageKB += usedStorageKB;
            return this;
        }

        public WarmupColRuleUsageData build()
        {
            return new WarmupColRuleUsageData(id, schema, table, column, warmUpType, priority, ttl, predicates, usedStorageKB);
        }
    }
}
