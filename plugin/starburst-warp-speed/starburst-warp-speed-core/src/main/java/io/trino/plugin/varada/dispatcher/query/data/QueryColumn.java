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
package io.trino.plugin.varada.dispatcher.query.data;

import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.spi.type.Type;

import java.util.Objects;
import java.util.Optional;

public abstract class QueryColumn
        implements QueryData
{
    protected final Optional<WarmUpElement> warmUpElementOptional;
    protected final VaradaColumn varadaColumn;
    protected final Type type;

    protected QueryColumn(WarmUpElement warmUpElement, Type type)
    {
        this.varadaColumn = warmUpElement.getVaradaColumn();
        this.type = type;
        this.warmUpElementOptional = Optional.of(warmUpElement);
    }

    protected QueryColumn(VaradaColumn varadaColumn, Type type)
    {
        this.varadaColumn = varadaColumn;
        this.type = type;
        this.warmUpElementOptional = Optional.empty();
    }

    public Optional<WarmUpElement> getWarmUpElementOptional()
    {
        return warmUpElementOptional;
    }

    public VaradaColumn getVaradaColumn()
    {
        return varadaColumn;
    }

    public Type getType()
    {
        return type;
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
        QueryColumn that = (QueryColumn) o;
        return Objects.equals(warmUpElementOptional, that.warmUpElementOptional) &&
                Objects.equals(varadaColumn, that.varadaColumn) &&
                Objects.equals(type, that.type);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(warmUpElementOptional, varadaColumn, type);
    }

    @Override
    public String toString()
    {
        return "QueryData{" +
                "warmUpElementOptional=" + warmUpElementOptional +
                ", varadaColumn=" + varadaColumn +
                ", type=" + type +
                '}';
    }

    public abstract Builder asBuilder();

    public abstract static class Builder
    {
        protected Optional<WarmUpElement> warmUpElementOptional;
        protected VaradaColumn varadaColumn;
        protected Type type;

        public Builder()
        {
        }

        public Builder(QueryColumn queryColumn)
        {
            this.warmUpElementOptional = queryColumn.warmUpElementOptional;
            this.varadaColumn = queryColumn.varadaColumn;
            this.type = queryColumn.type;
        }

        public Builder warmUpElementOptional(Optional<WarmUpElement> warmUpElementOptional)
        {
            this.warmUpElementOptional = warmUpElementOptional;
            return this;
        }

        public Builder varadaColumn(VaradaColumn varadaColumn)
        {
            this.varadaColumn = varadaColumn;
            return this;
        }

        public Builder type(Type type)
        {
            this.type = type;
            return this;
        }

        public abstract QueryColumn build();
    }
}
