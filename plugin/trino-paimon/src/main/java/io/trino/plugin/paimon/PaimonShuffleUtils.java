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
package io.trino.plugin.paimon;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.RowType;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Verify.verify;
import static io.trino.spi.block.RowBlock.getRowFieldsFromBlock;
import static java.util.Objects.requireNonNull;

final class PaimonShuffleUtils
{
    private PaimonShuffleUtils() {}

    static Page rowIdFieldPage(Page page)
    {
        requireNonNull(page, "page is null");
        List<Block> fieldBlocks = getRowFieldsFromBlock(page.getBlock(0));
        return new Page(page.getPositionCount(), fieldBlocks.toArray(Block[]::new));
    }

    static void validateRowIdType(RowType rowIdType, TableSchema schema)
    {
        requireNonNull(rowIdType, "rowIdType is null");
        requireNonNull(schema, "schema is null");
        List<DataField> primaryKeyFields = primaryKeyFields(schema);
        verify(rowIdType.getFields().size() == schema.primaryKeys().size(),
                "Paimon row id field count (%s) must match primary key count (%s)",
                rowIdType.getFields().size(),
                schema.primaryKeys().size());
        for (int index = 0; index < schema.primaryKeys().size(); index++) {
            String primaryKey = schema.primaryKeys().get(index);
            RowType.Field field = rowIdType.getFields().get(index);
            verify(field.getName().isPresent(),
                    "Paimon row id field at index %s must be named",
                    index);
            verify(field.getName().get().equals(primaryKey),
                    "Paimon row id field at index %s must be primary key '%s', got '%s'",
                    index,
                    primaryKey,
                    field.getName().get());
            DataType expectedType = primaryKeyFields.get(index).type();
            verify(PaimonColumnHandle.matchesTrinoType(expectedType, field.getType()),
                    "Paimon row id field '%s' type must match Paimon primary key type %s, got %s",
                    primaryKey,
                    expectedType.asSQLString(),
                    field.getType());
        }
    }

    static List<DataType> projectedTypes(TableSchema schema, List<String> fieldNames)
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(fieldNames, "fieldNames is null");
        return fieldNames.stream()
                .map(fieldName -> {
                    verify(schema.logicalRowType().containsField(fieldName),
                            "Paimon field '%s' is not present in table schema",
                            fieldName);
                    return schema.logicalRowType().getField(fieldName).type();
                })
                .toList();
    }

    static int[] projection(List<String> inputFields, List<String> projectedFields, String fieldDescription)
    {
        requireNonNull(inputFields, "inputFields is null");
        requireNonNull(projectedFields, "projectedFields is null");
        requireNonNull(fieldDescription, "fieldDescription is null");
        Map<String, Integer> inputFieldIndexes = new HashMap<>();
        for (int index = 0; index < inputFields.size(); index++) {
            inputFieldIndexes.putIfAbsent(inputFields.get(index), index);
        }
        return projectedFields.stream()
                .mapToInt(projectedField -> {
                    Integer index = inputFieldIndexes.get(projectedField);
                    verify(index != null,
                            "Paimon %s '%s' is not present in shuffle input fields %s",
                            fieldDescription,
                            projectedField,
                            inputFields);
                    return index;
                })
                .toArray();
    }

    private static List<DataField> primaryKeyFields(TableSchema schema)
    {
        return schema.primaryKeys().stream()
                .map(primaryKey -> {
                    verify(schema.logicalRowType().containsField(primaryKey),
                            "Paimon primary key '%s' is not present in table schema",
                            primaryKey);
                    return schema.logicalRowType().getField(primaryKey);
                })
                .toList();
    }
}
