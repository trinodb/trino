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

import io.trino.plugin.varada.dispatcher.SingleValue;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.spi.type.Type;

import java.util.Objects;

public class PrefilledQueryCollectData
        extends QueryCollectData
{
    private final SingleValue singleValue;

    private PrefilledQueryCollectData(VaradaColumn varadaColumn,
            Type type,
            int blockIndex,
            SingleValue singleValue)
    {
        super(varadaColumn, type, blockIndex);
        this.singleValue = singleValue;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public SingleValue getSingleValue()
    {
        return singleValue;
    }

    @Override
    public Builder asBuilder()
    {
        return new Builder(this);
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
        PrefilledQueryCollectData that = (PrefilledQueryCollectData) o;
        return super.equals(that) &&
                Objects.equals(singleValue, that.singleValue);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(), singleValue);
    }

    public static class Builder
            extends QueryCollectData.Builder
    {
        protected SingleValue singleValue;

        public Builder()
        {
            super();
        }

        public Builder(PrefilledQueryCollectData prefilledQueryCollectData)
        {
            super(prefilledQueryCollectData);
            this.singleValue = prefilledQueryCollectData.singleValue;
        }

        public Builder singleValue(SingleValue singleValue)
        {
            this.singleValue = singleValue;
            return this;
        }

        @Override
        public PrefilledQueryCollectData build()
        {
            return new PrefilledQueryCollectData(varadaColumn, type, blockIndex, singleValue);
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
        public Builder blockIndex(int blockIndex)
        {
            super.blockIndex(blockIndex);
            return this;
        }
    }
}
