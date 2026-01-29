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
package io.trino.plugin.weaviate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.weaviate.client6.v1.api.collections.DataType;
import io.weaviate.client6.v1.api.collections.Property;
import io.weaviate.client6.v1.api.collections.VectorConfig;
import io.weaviate.client6.v1.api.collections.VectorIndex;
import io.weaviate.client6.v1.api.collections.vectorindex.MultiVector;

import java.util.List;
import java.util.Map;

import static io.trino.plugin.weaviate.WeaviateErrorCode.WEAVIATE_MULTIPLE_DATA_TYPES;
import static io.trino.plugin.weaviate.WeaviateErrorCode.WEAVIATE_UNSUPPORTED_DATA_TYPE;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public record WeaviateColumnHandle(String name, Type trinoType)
        implements ColumnHandle
{
    static final WeaviateColumnHandle UUID = new WeaviateColumnHandle(Property.text("_id"));
    static final WeaviateColumnHandle CREATED_AT = new WeaviateColumnHandle(Property.date("_created_at"));
    static final WeaviateColumnHandle LAST_UPDATED_AT = new WeaviateColumnHandle(Property.date("_last_updated_at"));
    static final List<WeaviateColumnHandle> METADATA_COLUMNS = ImmutableList.of(UUID, CREATED_AT, LAST_UPDATED_AT);
    static final String VECTORS_COLUMN_NAME = "_vectors";

    @VisibleForTesting static final Type TEXT_ARRAY = new ArrayType(VARCHAR);
    @VisibleForTesting static final Type BOOL_ARRAY = new ArrayType(BOOLEAN);
    @VisibleForTesting static final Type INT_ARRAY = new ArrayType(INTEGER);
    @VisibleForTesting static final Type NUMBER_ARRAY = new ArrayType(DOUBLE);
    @VisibleForTesting static final Type DATE_ARRAY = new ArrayType(TIMESTAMP_MILLIS);
    @VisibleForTesting static final Type GEO_COORDINATES = RowType.rowType(
            RowType.field("latitude", DOUBLE),
            RowType.field("longitude", DOUBLE));
    @VisibleForTesting static final Type PHONE_NUMBER = RowType.rowType(
            RowType.field("defaultCountry", VARCHAR),
            RowType.field("countryCode", INTEGER),
            RowType.field("internationalFormatted", VARCHAR),
            RowType.field("national", INTEGER),
            RowType.field("nationalFormatted", VARCHAR),
            RowType.field("valid", BOOLEAN));

    @VisibleForTesting static final Type SINGLE_VECTOR = NUMBER_ARRAY;
    @VisibleForTesting static final Type MULTI_VECTOR = new ArrayType(NUMBER_ARRAY);

    public WeaviateColumnHandle
    {
        requireNonNull(name, "name is null");
        requireNonNull(trinoType, "trinoType is null");
    }

    public WeaviateColumnHandle(Property property)
    {
        this(property.propertyName(), toTrinoType(property));
    }

    public WeaviateColumnHandle(Map<String, VectorConfig> vectors)
    {
        this(VECTORS_COLUMN_NAME, toTrinoType(vectors));
    }

    private static Type toTrinoType(Property property)
    {
        requireNonNull(property, "property is null");

        List<String> dataTypes = property.dataTypes();
        if (dataTypes.size() > 1) {
            throw new TrinoException(WEAVIATE_MULTIPLE_DATA_TYPES, "Property %s has %d data types: %s".formatted(property.propertyName(), dataTypes.size(), property.dataTypes()));
        }
        String dataType = dataTypes.getFirst();
        return switch (dataType) {
            case DataType.TEXT, DataType.BLOB -> VARCHAR;
            case DataType.BOOL -> BOOLEAN;
            case DataType.INT -> INTEGER;
            case DataType.NUMBER -> DOUBLE;
            case DataType.DATE -> TIMESTAMP_MILLIS;
            case DataType.GEO_COORDINATES -> GEO_COORDINATES;
            case DataType.PHONE_NUMBER -> PHONE_NUMBER;
            case DataType.TEXT_ARRAY -> TEXT_ARRAY;
            case DataType.BOOL_ARRAY -> BOOL_ARRAY;
            case DataType.INT_ARRAY -> INT_ARRAY;
            case DataType.NUMBER_ARRAY -> NUMBER_ARRAY;
            case DataType.DATE_ARRAY -> DATE_ARRAY;
            case DataType.OBJECT -> nestedObjectType(property.nestedProperties());
            case DataType.OBJECT_ARRAY -> new ArrayType(nestedObjectType(property.nestedProperties()));
            default -> throw new TrinoException(WEAVIATE_UNSUPPORTED_DATA_TYPE, dataType + " is not supported");
        };
    }

    private static Type toTrinoType(Map<String, VectorConfig> vectors)
    {
        requireNonNull(vectors, "vectors is null");

        ImmutableList.Builder<RowType.Field> fields = ImmutableList.builder();
        vectors.forEach((vectorName, vectorConfig) -> {
            VectorIndex vectorIndex = vectorConfig.vectorIndex();
            MultiVector multiVector = switch (vectorIndex._kind()) {
                case HNSW -> vectorIndex.asHnsw().multiVector();
                case DYNAMIC -> vectorIndex.asDynamic().hnsw().multiVector();
                case FLAT -> null;
            };

            Type type = (multiVector == null || !multiVector.enabled())
                    ? SINGLE_VECTOR
                    : MULTI_VECTOR;

            fields.add(RowType.field(vectorName, type));
        });
        return RowType.from(fields.build());
    }

    private static RowType nestedObjectType(List<Property> properties)
    {
        requireNonNull(properties, "properties is null");

        ImmutableList.Builder<RowType.Field> fields = ImmutableList.builder();
        properties.forEach(property -> fields.add(
                RowType.field(property.propertyName(), toTrinoType(property))));
        return RowType.from(fields.build());
    }

    public ColumnMetadata columnMetadata()
    {
        return new ColumnMetadata(name, trinoType);
    }
}
