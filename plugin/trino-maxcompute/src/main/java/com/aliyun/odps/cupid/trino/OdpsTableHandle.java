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
package com.aliyun.odps.cupid.trino;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class OdpsTableHandle
        implements ConnectorTableHandle {
    private final String schemaName;
    private final String tableName;
    private final OdpsTable odpsTable;

    private final List<OdpsColumnHandle> desiredColumns;

    @JsonCreator
    public OdpsTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("odpsTable") OdpsTable odpsTable,
            @JsonProperty("desiredColumns") List<OdpsColumnHandle> desiredColumns) {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.odpsTable = requireNonNull(odpsTable, "odpsTable is null");
        this.desiredColumns = ImmutableList.copyOf(requireNonNull(desiredColumns, "desiredColumns is null"));
    }

    @JsonProperty
    public String getSchemaName() {
        return schemaName;
    }

    @JsonProperty
    public String getTableName() {
        return tableName;
    }

    @JsonProperty
    public OdpsTable getOdpsTable() {
        return odpsTable;
    }

    @JsonProperty
    public List<OdpsColumnHandle> getDesiredColumns() {
        return desiredColumns;
    }

    public SchemaTableName getSchemaTableName() {
        return new SchemaTableName(schemaName, tableName);
    }

    public SchemaTableName toSchemaTableName() {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaName, tableName, odpsTable, desiredColumns);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        OdpsTableHandle other = (OdpsTableHandle) obj;
        return Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName) &&
                Objects.equals(this.odpsTable, other.odpsTable) &&
                Objects.equals(this.desiredColumns, other.desiredColumns);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("schemaName", schemaName)
                .add("tableName", tableName)
                .toString();
    }
}
