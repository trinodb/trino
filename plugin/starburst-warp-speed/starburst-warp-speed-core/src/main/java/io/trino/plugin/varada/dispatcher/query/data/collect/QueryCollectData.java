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
package io.trino.plugin.varada.dispatcher.query.data.collect;

import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.dispatcher.query.data.QueryColumn;
import io.trino.spi.type.Type;

import java.util.Objects;

public abstract class QueryCollectData
        extends QueryColumn
{
    private final int blockIndex;

    protected QueryCollectData(WarmUpElement warmUpElement, Type type, int blockIndex)
    {
        super(warmUpElement, type);
        this.blockIndex = blockIndex;
    }

    protected QueryCollectData(VaradaColumn varadaColumn, Type type, int blockIndex)
    {
        super(varadaColumn, type);
        this.blockIndex = blockIndex;
    }

    public int getBlockIndex()
    {
        return blockIndex;
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
        QueryCollectData that = (QueryCollectData) o;
        return super.equals(that) && (blockIndex == that.blockIndex);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(), blockIndex);
    }

    @Override
    public String toString()
    {
        return "QueryCollectData{" +
                "blockIndex=" + blockIndex +
                "} " + super.toString();
    }

    @Override
    public abstract Builder asBuilder();

    public abstract static class Builder
            extends QueryColumn.Builder
    {
        protected int blockIndex;

        public Builder()
        {
            super();
        }

        public Builder(QueryCollectData queryCollectData)
        {
            super(queryCollectData);
            this.blockIndex = queryCollectData.blockIndex;
        }

        public Builder blockIndex(int blockIndex)
        {
            this.blockIndex = blockIndex;
            return this;
        }

        @Override
        public Builder varadaColumn(VaradaColumn varadaColumn)
        {
            super.varadaColumn(varadaColumn);
            return this;
        }

        @Override
        public Builder type(Type type)
        {
            super.type(type);
            return this;
        }

        @Override
        public abstract QueryCollectData build();
    }
}
