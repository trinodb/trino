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
package io.trino.plugin.cassandra;

import com.google.common.collect.ImmutableList;
import io.trino.spi.type.Type;

import java.util.List;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public record CassandraType(Kind kind, Type trinoType, List<CassandraType> argumentTypes)
{
    public enum Kind
    {
        BOOLEAN(true),
        TINYINT(true),
        SMALLINT(true),
        INT(true),
        BIGINT(true),
        FLOAT(true),
        DOUBLE(true),
        DECIMAL(true),
        DATE(true),
        TIME(true),
        TIMESTAMP(true),
        ASCII(true),
        TEXT(true),
        VARCHAR(true),
        BLOB(false),
        UUID(true),
        TIMEUUID(true),
        COUNTER(false),
        VARINT(false),
        INET(true),
        CUSTOM(false),
        LIST(false),
        SET(false),
        MAP(false),
        TUPLE(false),
        UDT(false),
        /**/;

        private final boolean supportedPartitionKey;

        Kind(boolean supportedPartitionKey)
        {
            this.supportedPartitionKey = supportedPartitionKey;
        }

        public boolean isSupportedPartitionKey()
        {
            return supportedPartitionKey;
        }
    }

    public CassandraType
    {
        requireNonNull(kind, "kind is null");
        requireNonNull(trinoType, "trinoType is null");
        argumentTypes = ImmutableList.copyOf(requireNonNull(argumentTypes, "argumentTypes is null"));
    }

    public static CassandraType primitiveType(Kind kind, Type trinoType)
    {
        return new CassandraType(kind, trinoType, ImmutableList.of());
    }

    @Override
    public String toString()
    {
        String result = format("%s(%s", kind, trinoType);
        if (!argumentTypes.isEmpty()) {
            result += "; " + argumentTypes;
        }
        result += ")";
        return result;
    }
}
