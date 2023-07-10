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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Objects;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class CassandraType
{
    public enum Kind
    {
        BOOLEAN,
        TINYINT,
        SMALLINT,
        INT,
        BIGINT,
        FLOAT,
        DOUBLE,
        DECIMAL,
        DATE,
        TIME,
        TIMESTAMP,
        ASCII,
        TEXT,
        VARCHAR,
        BLOB,
        UUID,
        TIMEUUID,
        COUNTER,
        VARINT,
        INET,
        CUSTOM,
        LIST,
        SET,
        MAP,
        TUPLE,
        UDT,
    }

    private final Kind kind;
    private final Type trinoType;
    private final List<CassandraType> argumentTypes;

    public CassandraType(
            Kind kind,
            Type trinoType)
    {
        this(kind, trinoType, ImmutableList.of());
    }

    @JsonCreator
    public CassandraType(
            @JsonProperty("kind") Kind kind,
            @JsonProperty("trinoType") Type trinoType,
            @JsonProperty("argumentTypes") List<CassandraType> argumentTypes)
    {
        this.kind = requireNonNull(kind, "kind is null");
        this.trinoType = requireNonNull(trinoType, "trinoType is null");
        this.argumentTypes = ImmutableList.copyOf(requireNonNull(argumentTypes, "argumentTypes is null"));
    }

    @JsonProperty
    public Kind getKind()
    {
        return kind;
    }

    @JsonProperty
    public Type getTrinoType()
    {
        return trinoType;
    }

    @JsonProperty
    public List<CassandraType> getArgumentTypes()
    {
        return argumentTypes;
    }

    public String getName()
    {
        return kind.name();
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
        CassandraType that = (CassandraType) o;
        return kind == that.kind && Objects.equals(trinoType, that.trinoType) && Objects.equals(argumentTypes, that.argumentTypes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(kind, trinoType, argumentTypes);
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
