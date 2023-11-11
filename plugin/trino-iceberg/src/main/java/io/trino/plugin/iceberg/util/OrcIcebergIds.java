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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.graph.Traverser;
import io.trino.orc.OrcColumn;
import io.trino.orc.OrcReader;
import io.trino.orc.metadata.OrcType.OrcTypeKind;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.NameMapping;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.iceberg.util.OrcTypeConverter.ORC_ICEBERG_ID_KEY;

public final class OrcIcebergIds
{
    private OrcIcebergIds() {}

    public static Map<Integer, OrcColumn> fileColumnsByIcebergId(OrcReader reader, Optional<NameMapping> nameMapping)
    {
        List<OrcColumn> fileColumns = reader.getRootColumn().getNestedColumns();

        if (nameMapping.isPresent() && !hasIds(reader.getRootColumn())) {
            fileColumns = fileColumns.stream()
                    .map(orcColumn -> setMissingFieldIds(orcColumn, nameMapping.get(), ImmutableList.of(orcColumn.getColumnName())))
                    .collect(toImmutableList());
        }

        return mapIdsToOrcFileColumns(fileColumns);
    }

    private static boolean hasIds(OrcColumn column)
    {
        if (column.getAttributes().containsKey(ORC_ICEBERG_ID_KEY)) {
            return true;
        }

        return column.getNestedColumns().stream().anyMatch(OrcIcebergIds::hasIds);
    }

    private static OrcColumn setMissingFieldIds(OrcColumn column, NameMapping nameMapping, List<String> qualifiedPath)
    {
        MappedField mappedField = nameMapping.find(qualifiedPath);

        ImmutableMap.Builder<String, String> attributes = ImmutableMap.builder();
        attributes.putAll(column.getAttributes());
        if ((mappedField != null) && (mappedField.id() != null)) {
            attributes.put(ORC_ICEBERG_ID_KEY, String.valueOf(mappedField.id()));
        }

        List<OrcColumn> orcColumns = column.getNestedColumns().stream()
                .map(nestedColumn -> setMissingFieldIds(nestedColumn, nameMapping, ImmutableList.<String>builder()
                        .addAll(qualifiedPath)
                        .add(pathName(column, nestedColumn))
                        .build()))
                .collect(toImmutableList());

        return new OrcColumn(
                column.getPath(),
                column.getColumnId(),
                column.getColumnName(),
                column.getColumnType(),
                column.getOrcDataSourceId(),
                orcColumns,
                attributes.buildOrThrow());
    }

    private static String pathName(OrcColumn column, OrcColumn nestedColumn)
    {
        // Trino ORC reader uses "item" for list element names, but NameMapper expects "element"
        if (column.getColumnType() == OrcTypeKind.LIST) {
            return "element";
        }
        return nestedColumn.getColumnName();
    }

    private static Map<Integer, OrcColumn> mapIdsToOrcFileColumns(List<OrcColumn> columns)
    {
        ImmutableMap.Builder<Integer, OrcColumn> columnsById = ImmutableMap.builder();
        Traverser.forTree(OrcColumn::getNestedColumns)
                .depthFirstPreOrder(columns)
                .forEach(column -> {
                    String fieldId = column.getAttributes().get(ORC_ICEBERG_ID_KEY);
                    if (fieldId != null) {
                        columnsById.put(Integer.parseInt(fieldId), column);
                    }
                });
        return columnsById.buildOrThrow();
    }
}
