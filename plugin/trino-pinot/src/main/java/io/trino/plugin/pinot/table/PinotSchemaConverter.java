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
package io.trino.plugin.pinot.table;

import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.type.Type;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.DateTimeGranularitySpec;
import org.apache.pinot.spi.data.FieldSpec.FieldType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.Schema.SchemaBuilder;

import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.pinot.PinotTableProperties.DATE_TIME_FORMAT_PROPERTY;
import static io.trino.plugin.pinot.PinotTableProperties.DATE_TIME_GRANULARITY_PROPERTY;
import static io.trino.plugin.pinot.PinotTableProperties.DATE_TIME_TRANSFORM_PROPERTY;
import static io.trino.plugin.pinot.PinotTableProperties.FIELD_TYPE_PROPERTY;
import static io.trino.plugin.pinot.PinotTableProperties.PINOT_COLUMN_NAME_PROPERTY;
import static io.trino.plugin.pinot.PinotTableProperties.SCHEMA_NAME_PROPERTY;
import static io.trino.plugin.pinot.PinotTypeUtils.getDefaultNullValue;
import static io.trino.plugin.pinot.PinotTypeUtils.getPinotType;
import static io.trino.plugin.pinot.PinotTypeUtils.getStringProperty;
import static io.trino.plugin.pinot.PinotTypeUtils.isSupportedPrimitive;
import static io.trino.plugin.pinot.PinotTypeUtils.isSupportedType;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PinotSchemaConverter
{
    private PinotSchemaConverter() {}

    public static Schema convert(ConnectorTableMetadata tableMetadata)
    {
        requireNonNull(tableMetadata, "tableMetadata is null");
        SchemaBuilder schemaBuilder = new SchemaBuilder();

        String schemaProperty = (String) tableMetadata.getProperties().get(SCHEMA_NAME_PROPERTY);
        String schemaName = tableMetadata.getTable().getTableName();
        if (!isNullOrEmpty(schemaProperty)) {
            schemaName = schemaProperty;
        }
        schemaBuilder.setSchemaName(schemaName);

        Map<String, ColumnMetadata> columns = tableMetadata.getColumns().stream()
                .collect(toImmutableMap(column -> column.getName(), column -> column));
        for (ColumnMetadata columnMetadata : columns.values()) {
            Type type = columnMetadata.getType();
            String pinotColumnName = getStringProperty(columnMetadata, PINOT_COLUMN_NAME_PROPERTY);
            if (pinotColumnName == null) {
                pinotColumnName = columnMetadata.getName();
            }
            checkState(isSupportedType(type), "Unsupported type %s for dimension column", type);
            boolean isSingleValueField = isSupportedPrimitive(type);
            FieldType fieldType = (FieldType) columnMetadata.getProperties().get(FIELD_TYPE_PROPERTY);
            switch (fieldType) {
                case DIMENSION:
                    validateNonDateTimeField(columnMetadata);
                    if (isSingleValueField) {
                        schemaBuilder.addSingleValueDimension(pinotColumnName, getPinotType(type), getDefaultNullValue(columnMetadata));
                    }
                    else {
                        schemaBuilder.addMultiValueDimension(pinotColumnName, getPinotType(getOnlyElement(type.getTypeParameters())));
                    }
                    break;
                case METRIC:
                    checkState(isSingleValueField, "Unsupported type %s for metric column", type);
                    validateNonDateTimeField(columnMetadata);
                    schemaBuilder.addMetric(pinotColumnName, getPinotType(type), getDefaultNullValue(columnMetadata));
                    break;
                case DATE_TIME:
                    checkState(isSingleValueField, "Unsupported type %s for date/time column", type);
                    String format = requireNonNull(getStringProperty(columnMetadata, DATE_TIME_FORMAT_PROPERTY), "required format property is null");
                    String granularity = getGranularity(columnMetadata);
                    String transform = getStringProperty(columnMetadata, DATE_TIME_TRANSFORM_PROPERTY);
                    schemaBuilder.addDateTime(pinotColumnName, getPinotType(type), format, granularity, getDefaultNullValue(columnMetadata), transform);
                    break;
                default:
                    throw new UnsupportedOperationException(format("Unsupported pinot field type '%s'", fieldType));
            }
        }

        return schemaBuilder.build();
    }

    private static void validateNonDateTimeField(ColumnMetadata columnMetadata)
    {
        checkState(getStringProperty(columnMetadata, DATE_TIME_FORMAT_PROPERTY) == null, "Unexpected property format");
        checkState(getStringProperty(columnMetadata, DATE_TIME_GRANULARITY_PROPERTY) == null, "Unexpected property granularity");
        checkState(getStringProperty(columnMetadata, DATE_TIME_TRANSFORM_PROPERTY) == null, "Unexpected property transform");
    }

    private static String getGranularity(ColumnMetadata columnMetadata)
    {
        String format = requireNonNull(getStringProperty(columnMetadata, DATE_TIME_FORMAT_PROPERTY), "required format property is null");
        String granularity = getStringProperty(columnMetadata, DATE_TIME_GRANULARITY_PROPERTY);
        if (granularity != null) {
            return granularity;
        }
        DateTimeFormatSpec formatSpec = new DateTimeFormatSpec(format);
        DateTimeGranularitySpec granularitySpec = new DateTimeGranularitySpec(formatSpec.getColumnSize(), formatSpec.getColumnUnit());
        return granularitySpec.getGranularity();
    }
}
