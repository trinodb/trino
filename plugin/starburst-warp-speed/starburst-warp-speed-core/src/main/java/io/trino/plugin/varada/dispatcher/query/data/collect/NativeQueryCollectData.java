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
import io.trino.plugin.varada.dispatcher.query.MatchCollectIdService;
import io.trino.plugin.varada.dispatcher.query.MatchCollectUtils.MatchCollectType;
import io.trino.spi.type.Type;

import java.util.Objects;
import java.util.Optional;

public class NativeQueryCollectData
        extends QueryCollectData
        implements Comparable<NativeQueryCollectData>
{
    protected final MatchCollectType matchCollectType;
    protected final int matchCollectId;

    private NativeQueryCollectData(WarmUpElement warmUpElement,
            Type type,
            int blockIndex,
            MatchCollectType matchCollectType,
            int matchCollectId)
    {
        super(warmUpElement, type, blockIndex);
        this.matchCollectType = matchCollectType;
        this.matchCollectId = matchCollectId;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    @Override
    public Builder asBuilder()
    {
        return new Builder(this);
    }

    public WarmUpElement getWarmUpElement()
    {
        // must present
        return getWarmUpElementOptional().orElseThrow();
    }

    public MatchCollectType getMatchCollectType()
    {
        return matchCollectType;
    }

    public int getMatchCollectId()
    {
        return matchCollectId;
    }

    @Override
    public int compareTo(NativeQueryCollectData o)
    {
        return Integer.compare(getWarmUpElement().getRecTypeLength(), o.getWarmUpElement().getRecTypeLength());
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
        NativeQueryCollectData that = (NativeQueryCollectData) o;
        return super.equals(that) &&
                this.matchCollectType == that.matchCollectType &&
                this.matchCollectId == that.matchCollectId;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(), matchCollectType, matchCollectId);
    }

    @Override
    public String toString()
    {
        return "NativeQueryCollectData{" +
                "matchCollectType=" + matchCollectType +
                "matchCollectId=" + matchCollectId +
                "} " + super.toString();
    }

    public static class Builder
            extends QueryCollectData.Builder
    {
        protected MatchCollectType matchCollectType = MatchCollectType.DISABLED;
        protected int matchCollectId = MatchCollectIdService.INVALID_ID;

        public Builder()
        {
            super();
        }

        public Builder(NativeQueryCollectData nativeQueryCollectData)
        {
            super(nativeQueryCollectData);

            this.matchCollectType = nativeQueryCollectData.matchCollectType;
            this.matchCollectId = nativeQueryCollectData.matchCollectId;
        }

        @Override
        public NativeQueryCollectData build()
        {
            return new NativeQueryCollectData(warmUpElementOptional.orElseThrow(),
                    type,
                    blockIndex,
                    matchCollectType,
                    matchCollectId);
        }

        public Builder matchCollectType(MatchCollectType matchCollectType)
        {
            this.matchCollectType = matchCollectType;
            return this;
        }

        public Builder matchCollectId(int matchCollectId)
        {
            this.matchCollectId = matchCollectId;
            return this;
        }

        public Builder warmUpElement(WarmUpElement warmUpElement)
        {
            super.warmUpElementOptional(Optional.of(warmUpElement));
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
        public Builder blockIndex(int blockIndex)
        {
            super.blockIndex(blockIndex);
            return this;
        }
    }
}
