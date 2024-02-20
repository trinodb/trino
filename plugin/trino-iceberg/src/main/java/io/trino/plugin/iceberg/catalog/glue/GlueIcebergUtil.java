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
package io.trino.plugin.iceberg.catalog.glue;

import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.TableInput;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.iceberg.TypeConverter;
import io.trino.spi.type.TypeManager;
import jakarta.annotation.Nullable;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.collect.ImmutableList.builderWithExpectedSize;
import static io.trino.plugin.hive.HiveMetadata.PRESTO_VIEW_EXPANDED_TEXT_MARKER;
import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.hive.TableType.VIRTUAL_VIEW;
import static io.trino.plugin.hive.ViewReaderUtil.ICEBERG_MATERIALIZED_VIEW_COMMENT;
import static io.trino.plugin.iceberg.IcebergUtil.COLUMN_TRINO_NOT_NULL_PROPERTY;
import static io.trino.plugin.iceberg.IcebergUtil.COLUMN_TRINO_TYPE_ID_PROPERTY;
import static io.trino.plugin.iceberg.IcebergUtil.TRINO_TABLE_METADATA_INFO_VALID_FOR;
import static java.util.Locale.ENGLISH;
import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;

public final class GlueIcebergUtil
{
    private GlueIcebergUtil() {}

    // Limit per Glue API docs (https://docs.aws.amazon.com/glue/latest/webapi/API_TableInput.html#Glue-Type-TableInput-Parameters as of this writing)
    private static final int GLUE_TABLE_PARAMETER_LENGTH_LIMIT = 512000;
    // Limit per Glue API docs (https://docs.aws.amazon.com/glue/latest/webapi/API_Column.html as of this writing)
    private static final int GLUE_COLUMN_NAME_LENGTH_LIMIT = 255;
    // Limit per Glue API docs (https://docs.aws.amazon.com/glue/latest/webapi/API_Column.html as of this writing)
    private static final int GLUE_COLUMN_TYPE_LENGTH_LIMIT = 131072;
    // Limit per Glue API docs (https://docs.aws.amazon.com/glue/latest/webapi/API_Column.html as of this writing)
    private static final int GLUE_COLUMN_COMMENT_LENGTH_LIMIT = 255;
    // Limit per Glue API docs (https://docs.aws.amazon.com/glue/latest/webapi/API_Column.html as of this writing)
    private static final int GLUE_COLUMN_PARAMETER_LENGTH_LIMIT = 512000;

    public static TableInput getTableInput(
            TypeManager typeManager,
            String tableName,
            Optional<String> owner,
            TableMetadata metadata,
            String newMetadataLocation,
            Map<String, String> parameters,
            boolean cacheTableMetadata)
    {
        parameters = new HashMap<>(parameters);
        parameters.putIfAbsent(TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE.toUpperCase(ENGLISH));
        parameters.put(METADATA_LOCATION_PROP, newMetadataLocation);
        parameters.remove(TRINO_TABLE_METADATA_INFO_VALID_FOR); // no longer valid

        TableInput tableInput = new TableInput()
                .withName(tableName)
                .withOwner(owner.orElse(null))
                // Iceberg does not distinguish managed and external tables, all tables are treated the same and marked as EXTERNAL
                .withTableType(EXTERNAL_TABLE.name());

        if (cacheTableMetadata) {
            // Store table metadata sufficient to answer information_schema.columns and system.metadata.table_comments queries, which are often queried in bulk by e.g. BI tools
            String comment = metadata.properties().get(TABLE_COMMENT);
            Optional<List<Column>> glueColumns = glueColumns(typeManager, metadata);

            boolean canPersistComment = (comment == null || comment.length() <= GLUE_TABLE_PARAMETER_LENGTH_LIMIT);
            boolean canPersistColumnInfo = glueColumns.isPresent();
            boolean canPersistMetadata = canPersistComment && canPersistColumnInfo;

            if (canPersistMetadata) {
                tableInput.withStorageDescriptor(new StorageDescriptor()
                        .withColumns(glueColumns.get()));

                if (comment != null) {
                    parameters.put(TABLE_COMMENT, comment);
                }
                else {
                    parameters.remove(TABLE_COMMENT);
                }
                parameters.put(TRINO_TABLE_METADATA_INFO_VALID_FOR, newMetadataLocation);
            }
        }

        tableInput.withParameters(parameters);

        return tableInput;
    }

    private static Optional<List<Column>> glueColumns(TypeManager typeManager, TableMetadata metadata)
    {
        List<Types.NestedField> icebergColumns = metadata.schema().columns();
        ImmutableList.Builder<Column> glueColumns = builderWithExpectedSize(icebergColumns.size());

        boolean firstColumn = true;
        for (Types.NestedField icebergColumn : icebergColumns) {
            String glueTypeString = toGlueTypeStringLossy(icebergColumn.type());
            if (icebergColumn.name().length() > GLUE_COLUMN_NAME_LENGTH_LIMIT ||
                    firstNonNull(icebergColumn.doc(), "").length() > GLUE_COLUMN_COMMENT_LENGTH_LIMIT ||
                    glueTypeString.length() > GLUE_COLUMN_TYPE_LENGTH_LIMIT) {
                return Optional.empty();
            }
            String trinoTypeId = TypeConverter.toTrinoType(icebergColumn.type(), typeManager).getTypeId().getId();
            Column column = new Column()
                    .withName(icebergColumn.name())
                    .withType(glueTypeString)
                    .withComment(icebergColumn.doc());

            ImmutableMap.Builder<String, String> parameters = ImmutableMap.builder();
            if (icebergColumn.isRequired()) {
                parameters.put(COLUMN_TRINO_NOT_NULL_PROPERTY, "true");
            }
            if (firstColumn || !glueTypeString.equals(trinoTypeId)) {
                if (trinoTypeId.length() > GLUE_COLUMN_PARAMETER_LENGTH_LIMIT) {
                    return Optional.empty();
                }
                // Store type parameter for some (first) column so that we can later detect whether column parameters weren't erased by something.
                parameters.put(COLUMN_TRINO_TYPE_ID_PROPERTY, trinoTypeId);
            }
            column.setParameters(parameters.buildOrThrow());
            glueColumns.add(column);

            firstColumn = false;
        }

        return Optional.of(glueColumns.build());
    }

    // Copied from org.apache.iceberg.aws.glue.IcebergToGlueConverter#toTypeString
    private static String toGlueTypeStringLossy(Type type)
    {
        switch (type.typeId()) {
            case BOOLEAN:
                return "boolean";
            case INTEGER:
                return "int";
            case LONG:
                return "bigint";
            case FLOAT:
                return "float";
            case DOUBLE:
                return "double";
            case DATE:
                return "date";
            case TIME:
            case STRING:
            case UUID:
                return "string";
            case TIMESTAMP:
                return "timestamp";
            case FIXED:
            case BINARY:
                return "binary";
            case DECIMAL:
                final Types.DecimalType decimalType = (Types.DecimalType) type;
                return String.format("decimal(%s,%s)", decimalType.precision(), decimalType.scale());
            case STRUCT:
                final Types.StructType structType = type.asStructType();
                final String nameToType =
                        structType.fields().stream()
                                .map(f -> String.format("%s:%s", f.name(), toGlueTypeStringLossy(f.type())))
                                .collect(Collectors.joining(","));
                return String.format("struct<%s>", nameToType);
            case LIST:
                final Types.ListType listType = type.asListType();
                return String.format("array<%s>", toGlueTypeStringLossy(listType.elementType()));
            case MAP:
                final Types.MapType mapType = type.asMapType();
                return String.format(
                        "map<%s,%s>", toGlueTypeStringLossy(mapType.keyType()), toGlueTypeStringLossy(mapType.valueType()));
            default:
                return type.typeId().name().toLowerCase(Locale.ENGLISH);
        }
    }

    public static TableInput getViewTableInput(String viewName, String viewOriginalText, @Nullable String owner, Map<String, String> parameters)
    {
        return new TableInput()
                .withName(viewName)
                .withTableType(VIRTUAL_VIEW.name())
                .withViewOriginalText(viewOriginalText)
                .withViewExpandedText(PRESTO_VIEW_EXPANDED_TEXT_MARKER)
                .withOwner(owner)
                .withParameters(parameters);
    }

    public static TableInput getMaterializedViewTableInput(String viewName, String viewOriginalText, String owner, Map<String, String> parameters)
    {
        return new TableInput()
                .withName(viewName)
                .withTableType(VIRTUAL_VIEW.name())
                .withViewOriginalText(viewOriginalText)
                .withViewExpandedText(ICEBERG_MATERIALIZED_VIEW_COMMENT)
                .withOwner(owner)
                .withParameters(parameters);
    }
}
