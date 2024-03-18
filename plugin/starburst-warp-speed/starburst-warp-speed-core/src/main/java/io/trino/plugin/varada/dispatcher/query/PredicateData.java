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
package io.trino.plugin.varada.dispatcher.query;

import io.trino.spi.type.Type;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class PredicateData
{
    private final PredicateInfo predicateInfo;
    private final boolean collectNulls;
    private final int predicateHashCode;
    private final int predicateSize;
    private final Type columnType;

    private PredicateData(PredicateInfo predicateInfo,
            boolean collectNulls,
            int predicateHashCode,
            int predicateSize,
            Type columnType)
    {
        this.predicateInfo = requireNonNull(predicateInfo);
        this.collectNulls = collectNulls;
        this.predicateHashCode = predicateHashCode;
        this.predicateSize = predicateSize;
        this.columnType = requireNonNull(columnType);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public boolean isCollectNulls()
    {
        return collectNulls;
    }

    public PredicateInfo getPredicateInfo()
    {
        return predicateInfo;
    }

    public int getPredicateHashCode()
    {
        return predicateHashCode;
    }

    public int getPredicateSize()
    {
        return predicateSize;
    }

    public Type getColumnType()
    {
        return columnType;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PredicateData that)) {
            return false;
        }
        return (predicateHashCode == that.predicateHashCode &&
                collectNulls == that.collectNulls &&
                columnType == that.columnType &&
                predicateInfo == null) ? that.predicateInfo == null : predicateInfo.predicateType() == that.predicateInfo.predicateType();
    }

    @Override
    public String toString()
    {
        return "PredicateData{" +
                ", predicateInfo=" + predicateInfo +
                ", collectNulls=" + collectNulls +
                ", predicateHashCode=" + predicateHashCode +
                ", predicateSize=" + predicateSize +
                ", columnType=" + columnType +
                '}';
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(predicateInfo, predicateHashCode, predicateSize);
    }

    public static class Builder
    {
        private boolean isCollectNulls;
        private int predicateHashCode;
        private PredicateInfo predicateInfo;
        private int predicateSize;
        private Type columnType;

        private Builder()
        {}

        public Builder predicateInfo(PredicateInfo predicateInfo)
        {
            this.predicateInfo = predicateInfo;
            return this;
        }

        public Builder predicateSize(int predicateSize)
        {
            this.predicateSize = predicateSize;
            return this;
        }

        public Builder isCollectNulls(boolean isCollectNulls)
        {
            this.isCollectNulls = isCollectNulls;
            return this;
        }

        public Builder predicateHashCode(int predicateHashCode)
        {
            this.predicateHashCode = predicateHashCode;
            return this;
        }

        public Builder columnType(Type columnType)
        {
            this.columnType = columnType;
            return this;
        }

        public PredicateData build()
        {
            return new PredicateData(predicateInfo,
                    isCollectNulls,
                    predicateHashCode,
                    predicateSize,
                    columnType);
        }
    }
}
