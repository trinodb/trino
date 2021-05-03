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
package io.trino.orc;

import com.google.common.collect.Maps;
import io.trino.orc.OrcReader.FieldMapper;

import java.util.Locale;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class NameBasedFieldMapper
        implements FieldMapper
{
    private final Map<String, OrcColumn> nestedColumns;

    private NameBasedFieldMapper(Map<String, OrcColumn> nestedColumns)
    {
        this.nestedColumns = requireNonNull(nestedColumns, "nestedColumns is null");
    }

    @Override
    public OrcColumn get(String fieldName)
    {
        return nestedColumns.get(fieldName);
    }

    public static FieldMapper create(OrcColumn column)
    {
        requireNonNull(column, "column is null");
        Map<String, OrcColumn> nestedColumns = Maps.uniqueIndex(
                column.getNestedColumns(),
                field -> field.getColumnName().toLowerCase(Locale.ENGLISH));

        return new NameBasedFieldMapper(nestedColumns);
    }
}
