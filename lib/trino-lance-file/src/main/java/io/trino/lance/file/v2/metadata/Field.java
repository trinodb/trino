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
package io.trino.lance.file.v2.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class Field
{
    private final String name;
    private final int id;
    private final int parentId;
    private final String logicalType;
    private final Map<String, String> metadata;
    private final boolean nullable;
    private final List<Field> children;

    @JsonCreator
    public Field(
            @JsonProperty("name") String name,
            @JsonProperty("id") int id,
            @JsonProperty("parent") int parentId,
            @JsonProperty("logicalType") String logicalType,
            @JsonProperty("metadata") Map<String, String> metadata,
            @JsonProperty("nullable") boolean nullable,
            @JsonProperty("children") List<Field> children)
    {
        this.name = requireNonNull(name, "name is null");
        this.id = id;
        this.parentId = parentId;
        this.logicalType = requireNonNull(logicalType, "logicalType is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.nullable = nullable;
        this.children = requireNonNull(children, "children is null");
    }

    public static Field fromProto(build.buf.gen.lance.file.Field proto)
    {
        ImmutableMap.Builder<String, String> metadataBuilder = ImmutableMap.builder();
        for (Map.Entry<String, ByteString> entry : proto.getMetadataMap().entrySet()) {
            metadataBuilder.put(entry.getKey(), entry.getValue().toStringUtf8());
        }
        Map<String, String> metadata = metadataBuilder.buildOrThrow();
        return new Field(proto.getName(),
                proto.getId(),
                proto.getParentId(),
                proto.getLogicalType(),
                metadata,
                proto.getNullable(),
                new ArrayList());
    }

    public void addChild(Field child)
    {
        this.children.add(child);
    }

    @JsonProperty
    public int getId()
    {
        return id;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public int getParentId()
    {
        return parentId;
    }

    @JsonProperty
    public String getLogicalType()
    {
        return logicalType;
    }

    @JsonProperty
    public Map<String, String> getMetadata()
    {
        return metadata;
    }

    @JsonProperty
    public boolean isNullable()
    {
        return nullable;
    }

    @JsonProperty
    public List<Field> getChildren()
    {
        return children;
    }

    public boolean isLeaf()
    {
        return children.isEmpty();
    }

    public Type toTrinoType()
    {
        return switch (LogicalType.from(logicalType)) {
            case LogicalType.Int8Type _ -> TINYINT;
            case LogicalType.Int16Type _ -> SMALLINT;
            case LogicalType.Int32Type _ -> INTEGER;
            case LogicalType.Int64Type _ -> BIGINT;
            case LogicalType.FloatType _ -> REAL;
            case LogicalType.DoubleType _ -> DOUBLE;
            case LogicalType.StringType _ -> VARCHAR;
            case LogicalType.BinaryType _ -> VARBINARY;
            case LogicalType.StructType _ -> {
                List<RowType.Field> fields = children.stream()
                        .map(field -> RowType.field(field.name, field.toTrinoType()))
                        .collect(toImmutableList());
                yield RowType.from(fields);
            }
            case LogicalType.ListType _ -> {
                checkArgument(children.size() == 1);
                yield new ArrayType(children.get(0).toTrinoType());
            }
            case LogicalType.DateType _ -> DATE;
            default -> throw new IllegalArgumentException("Unsupported type: " + logicalType);
        };
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("id", id)
                .add("parentId", parentId)
                .add("logicalType", logicalType)
                .add("metadata", metadata)
                .add("nullable", nullable)
                .add("children", children)
                .toString();
    }
}
