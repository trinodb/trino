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

import static java.util.Objects.requireNonNull;

public class WarmupColRuleData
{
    private final int id;
    private final String schema;
    private final String table;
    private final VaradaColumnData column;
    private final ImmutableSet<WarmupPredicateRule> predicates;
    private final WarmUpType warmUpType;
    private final double priority;
    private final Duration ttl;

    @JsonCreator
    public WarmupColRuleData(@JsonProperty("id") int id,
            @JsonProperty("schema") String schema,
            @JsonProperty("table") String table,
            @JsonProperty("column") VaradaColumnData column,
            @JsonProperty("warmUpType") WarmUpType warmUpType,
            @JsonProperty("priority") double priority,
            @JsonProperty("ttl") Duration ttl,
            @JsonProperty("predicates") ImmutableSet<WarmupPredicateRule> predicates)
    {
        this.id = id;
        this.column = requireNonNull(column);
        this.schema = requireNonNull(schema);
        this.table = requireNonNull(table);
        this.warmUpType = requireNonNull(warmUpType);
        this.priority = priority;
        this.ttl = ttl;
        this.predicates = requireNonNull(predicates);
    }

    @JsonProperty
    public int getId()
    {
        return id;
    }

    @JsonProperty
    public VaradaColumnData getColumn()
    {
        return column;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public String getTable()
    {
        return table;
    }

    @JsonProperty
    public WarmUpType getWarmUpType()
    {
        return warmUpType;
    }

    @JsonProperty
    public double getPriority()
    {
        return priority;
    }

    @JsonProperty
    public Duration getTtl()
    {
        return ttl;
    }

    @JsonProperty
    public ImmutableSet<WarmupPredicateRule> getPredicates()
    {
        return predicates;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof WarmupColRuleData that)) {
            return false;
        }
        return getId() == that.getId() &&
                Double.compare(that.getPriority(), getPriority()) == 0 &&
                Objects.equals(getSchema(), that.getSchema()) &&
                Objects.equals(getTable(), that.getTable()) &&
                Objects.equals(getColumn(), that.getColumn()) &&
                Objects.equals(getPredicates(), that.getPredicates()) &&
                getWarmUpType() == that.getWarmUpType() &&
                Objects.equals(getTtl(), that.getTtl());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getId(), getSchema(), getTable(), getColumn(), getPredicates(), getWarmUpType(), getPriority(), getTtl());
    }

    @Override
    public String toString()
    {
        return "WarmupColRuleData{" +
                "id=" + id +
                ", schema='" + schema + '\'' +
                ", table='" + table + '\'' +
                ", column='" + column + '\'' +
                ", predicates=" + predicates +
                ", warmUpType=" + warmUpType +
                ", priority=" + priority +
                ", ttl=" + ttl +
                '}';
    }
}
