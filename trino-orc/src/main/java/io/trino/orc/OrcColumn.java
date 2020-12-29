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
package io.prestosql.orc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.orc.metadata.OrcColumnId;
import io.prestosql.orc.metadata.OrcType.OrcTypeKind;

import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class OrcColumn
{
    private final String path;
    private final OrcColumnId columnId;
    private final OrcTypeKind columnType;
    private final String columnName;
    private final OrcDataSourceId orcDataSourceId;
    private final List<OrcColumn> nestedColumns;
    private final Map<String, String> attributes;

    public OrcColumn(
            String path,
            OrcColumnId columnId,
            String columnName,
            OrcTypeKind columnType,
            OrcDataSourceId orcDataSourceId,
            List<OrcColumn> nestedColumns,
            Map<String, String> attributes)
    {
        this.path = requireNonNull(path, "path is null");
        this.columnId = requireNonNull(columnId, "columnId is null");
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.orcDataSourceId = requireNonNull(orcDataSourceId, "orcDataSourceId is null");
        this.nestedColumns = ImmutableList.copyOf(requireNonNull(nestedColumns, "nestedColumns is null"));
        this.attributes = ImmutableMap.copyOf(requireNonNull(attributes, "attributes is null"));
    }

    public String getPath()
    {
        return path;
    }

    public OrcColumnId getColumnId()
    {
        return columnId;
    }

    public OrcTypeKind getColumnType()
    {
        return columnType;
    }

    public String getColumnName()
    {
        return columnName;
    }

    public OrcDataSourceId getOrcDataSourceId()
    {
        return orcDataSourceId;
    }

    public List<OrcColumn> getNestedColumns()
    {
        return nestedColumns;
    }

    public Map<String, String> getAttributes()
    {
        return attributes;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("path", path)
                .add("columnId", columnId)
                .add("streamType", columnType)
                .add("dataSource", orcDataSourceId)
                .toString();
    }
}
