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
package io.trino.plugin.varada.warmup.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.warp.gen.constants.WarmUpType;

import java.util.Locale;
import java.util.Objects;
import java.util.Set;

import static java.util.Objects.requireNonNull;

@JsonDeserialize(builder = WarmupRule.Builder.class)
public class WarmupRule
{
    public static final String ID = "id";
    public static final String SCHEMA_NAME = "schema_name";
    public static final String TABLE_NAME = "table_name";
    public static final String VARADA_COLUMN = "varada_column";
    public static final String TTL = "ttl";
    public static final String PRIORITY = "priority";
    public static final String COLUMN_WARMUP_TYPE = "column_warmup_type";
    public static final String PREDICATES = "predicates";

    @JsonProperty(ID)
    private int id;

    @JsonProperty(SCHEMA_NAME)
    private String schema;

    @JsonProperty(TABLE_NAME)
    private String table;

    @JsonProperty(VARADA_COLUMN)
    private VaradaColumn varadaColumn;

    @JsonProperty(TTL)
    private int ttl;

    @JsonProperty(PRIORITY)
    private double priority;

    @JsonProperty(COLUMN_WARMUP_TYPE)
    private WarmUpType warmUpType;

    @JsonProperty(PREDICATES)
    private Set<WarmupPredicateRule> predicates;

    @SuppressWarnings("unused")
    private WarmupRule() {}

    private WarmupRule(
            int id,
            String schema,
            String table,
            VaradaColumn varadaColumn,
            WarmUpType warmUpType,
            double priority,
            int ttl,
            Set<WarmupPredicateRule> predicates)
    {
        this.id = id;
        this.schema = requireNonNull(schema).toLowerCase(Locale.ENGLISH);
        this.table = requireNonNull(table).toLowerCase(Locale.ENGLISH);
        this.varadaColumn = requireNonNull(varadaColumn);
        this.warmUpType = warmUpType;
        this.priority = priority;
        this.ttl = ttl;
        this.predicates = predicates;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builder(WarmupRule warmupRule)
    {
        return new Builder()
                .id(warmupRule.getId())
                .schema(warmupRule.getSchema())
                .table(warmupRule.getTable())
                .warmUpType(warmupRule.getWarmUpType())
                .varadaColumn(warmupRule.getVaradaColumn())
                .priority(warmupRule.getPriority())
                .ttl(warmupRule.getTtl())
                .predicates(warmupRule.getPredicates());
    }

    public int getId()
    {
        return id;
    }

    public String getSchema()
    {
        return schema;
    }

    public String getTable()
    {
        return table;
    }

    public VaradaColumn getVaradaColumn()
    {
        return varadaColumn;
    }

    public int getTtl()
    {
        return ttl;
    }

    public double getPriority()
    {
        return priority;
    }

    public WarmUpType getWarmUpType()
    {
        return warmUpType;
    }

    public Set<WarmupPredicateRule> getPredicates()
    {
        return predicates;
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
        WarmupRule that = (WarmupRule) o;
        return id == that.id && ttl == that.ttl && Double.compare(that.priority, priority) == 0 && Objects.equals(schema, that.schema) && Objects.equals(table, that.table) && Objects.equals(varadaColumn, that.varadaColumn) && warmUpType == that.warmUpType && Objects.equals(predicates, that.predicates);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, schema, table, varadaColumn, ttl, priority, warmUpType, predicates);
    }

    @Override
    public String toString()
    {
        return "WarmupRule{" +
                "id=" + id +
                ", schema='" + schema + '\'' +
                ", table='" + table + '\'' +
                ", varadaColumn='" + varadaColumn + '\'' +
                ", warmUpType=" + warmUpType +
                ", priority=" + priority +
                ", ttl=" + ttl +
                ", predicates=" + predicates +
                '}';
    }

    @JsonPOJOBuilder
    public static class Builder
    {
        private int id;
        private String schema;
        private String table;
        private VaradaColumn varadaColumn;
        private WarmUpType warmUpType;
        private double priority;
        private int ttl;
        private Set<WarmupPredicateRule> predicates;

        private Builder()
        {
        }

        @JsonProperty(ID)
        public Builder id(int id)
        {
            this.id = id;
            return this;
        }

        @JsonProperty(VARADA_COLUMN)
        public Builder varadaColumn(VaradaColumn varadaColumn)
        {
            this.varadaColumn = requireNonNull(varadaColumn);
            return this;
        }

        @JsonProperty(SCHEMA_NAME)
        public Builder schema(String schema)
        {
            this.schema = requireNonNull(schema);
            return this;
        }

        @JsonProperty(TABLE_NAME)
        public Builder table(String table)
        {
            this.table = requireNonNull(table);
            return this;
        }

        @JsonProperty(COLUMN_WARMUP_TYPE)
        public Builder warmUpType(WarmUpType warmUpType)
        {
            this.warmUpType = warmUpType;
            return this;
        }

        @JsonProperty(PRIORITY)
        public Builder priority(double priority)
        {
            this.priority = priority;
            return this;
        }

        @JsonProperty(TTL)
        public Builder ttl(int ttl)
        {
            this.ttl = ttl;
            return this;
        }

        @JsonProperty(PREDICATES)
        public Builder predicates(Set<WarmupPredicateRule> predicates)
        {
            this.predicates = requireNonNull(predicates);
            return this;
        }

        public WarmupRule build()
        {
            return new WarmupRule(id, schema, table, varadaColumn, warmUpType, priority, ttl, predicates);
        }
    }
}
