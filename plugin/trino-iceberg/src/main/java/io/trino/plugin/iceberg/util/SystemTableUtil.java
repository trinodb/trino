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
package io.trino.plugin.iceberg.util;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.base.util.JsonUtils;
import io.trino.plugin.iceberg.system.IcebergPartitionColumn;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.MetricsUtil;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SingleValueParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Type.PrimitiveType;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types.NestedField;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.iceberg.TypeConverter.toTrinoType;

public final class SystemTableUtil
{
    private static final JsonFactory JSON_FACTORY = JsonUtils.jsonFactoryBuilder().build();

    private SystemTableUtil() {}

    public static List<PartitionField> getAllPartitionFields(Table icebergTable)
    {
        return getAllPartitionFields(icebergTable.schema(), icebergTable.specs());
    }

    public static List<PartitionField> getAllPartitionFields(Schema schema, Map<Integer, PartitionSpec> specs)
    {
        Set<Integer> existingColumnsIds = TypeUtil.indexById(schema.asStruct()).keySet();

        List<PartitionField> visiblePartitionFields = specs
                .values().stream()
                .flatMap(partitionSpec -> partitionSpec.fields().stream())
                // skip columns that were dropped
                .filter(partitionField -> existingColumnsIds.contains(partitionField.sourceId()))
                .collect(toImmutableList());

        return filterOutDuplicates(visiblePartitionFields);
    }

    private static List<PartitionField> filterOutDuplicates(List<PartitionField> visiblePartitionFields)
    {
        Set<Integer> alreadyExistingFieldIds = new HashSet<>();
        List<PartitionField> result = new ArrayList<>();
        for (PartitionField partitionField : visiblePartitionFields) {
            if (!alreadyExistingFieldIds.contains(partitionField.fieldId())) {
                alreadyExistingFieldIds.add(partitionField.fieldId());
                result.add(partitionField);
            }
        }
        return result;
    }

    public static Optional<IcebergPartitionColumn> getPartitionColumnType(TypeManager typeManager, List<PartitionField> fields, Schema schema)
    {
        if (fields.isEmpty()) {
            return Optional.empty();
        }
        List<RowType.Field> partitionFields = fields.stream()
                .map(field -> RowType.field(
                        field.name(),
                        toTrinoType(field.transform().getResultType(schema.findType(field.sourceId())), typeManager)))
                .collect(toImmutableList());
        List<Integer> fieldIds = fields.stream()
                .map(PartitionField::fieldId)
                .collect(toImmutableList());
        return Optional.of(new IcebergPartitionColumn(RowType.from(partitionFields), fieldIds));
    }

    public static List<Type> partitionTypes(List<PartitionField> partitionFields, Map<Integer, PrimitiveType> idToPrimitiveTypeMapping)
    {
        ImmutableList.Builder<Type> partitionTypeBuilder = ImmutableList.builder();
        for (PartitionField partitionField : partitionFields) {
            PrimitiveType sourceType = idToPrimitiveTypeMapping.get(partitionField.sourceId());
            Type type = partitionField.transform().getResultType(sourceType);
            partitionTypeBuilder.add(type);
        }
        return partitionTypeBuilder.build();
    }

    public static String readableMetricsToJson(MetricsUtil.ReadableMetricsStruct readableMetrics, List<NestedField> primitiveFields)
    {
        StringWriter writer = new StringWriter();
        try {
            JsonGenerator generator = JSON_FACTORY.createGenerator(writer);
            generator.writeStartObject();

            for (int i = 0; i < readableMetrics.size(); i++) {
                NestedField field = primitiveFields.get(i);
                generator.writeFieldName(field.name());

                generator.writeStartObject();
                MetricsUtil.ReadableColMetricsStruct columnMetrics = readableMetrics.get(i, MetricsUtil.ReadableColMetricsStruct.class);

                generator.writeFieldName("column_size");
                Long columnSize = columnMetrics.get(0, Long.class);
                if (columnSize == null) {
                    generator.writeNull();
                }
                else {
                    generator.writeNumber(columnSize);
                }

                generator.writeFieldName("value_count");
                Long valueCount = columnMetrics.get(1, Long.class);
                if (valueCount == null) {
                    generator.writeNull();
                }
                else {
                    generator.writeNumber(valueCount);
                }

                generator.writeFieldName("null_value_count");
                Long nullValueCount = columnMetrics.get(2, Long.class);
                if (nullValueCount == null) {
                    generator.writeNull();
                }
                else {
                    generator.writeNumber(nullValueCount);
                }

                generator.writeFieldName("nan_value_count");
                Long nanValueCount = columnMetrics.get(3, Long.class);
                if (nanValueCount == null) {
                    generator.writeNull();
                }
                else {
                    generator.writeNumber(nanValueCount);
                }

                generator.writeFieldName("lower_bound");
                SingleValueParser.toJson(field.type(), columnMetrics.get(4, Object.class), generator);

                generator.writeFieldName("upper_bound");
                SingleValueParser.toJson(field.type(), columnMetrics.get(5, Object.class), generator);

                generator.writeEndObject();
            }

            generator.writeEndObject();
            generator.flush();
            return writer.toString();
        }
        catch (IOException e) {
            throw new UncheckedIOException("JSON conversion failed for: " + readableMetrics, e);
        }
    }
}
