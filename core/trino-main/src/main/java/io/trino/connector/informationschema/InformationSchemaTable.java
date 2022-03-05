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
package io.trino.connector.informationschema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.trino.metadata.MetadataUtil.TableMetadataBuilder;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;

import java.util.Optional;

import static io.trino.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public enum InformationSchemaTable
{
    COLUMNS(table("columns")
            .column("table_catalog", createUnboundedVarcharType())
            .column("table_schema", createUnboundedVarcharType())
            .column("table_name", createUnboundedVarcharType())
            .column("column_name", createUnboundedVarcharType())
            .column("ordinal_position", BIGINT)
            .column("column_default", createUnboundedVarcharType())
            .column("is_nullable", createUnboundedVarcharType())
            .column("data_type", createUnboundedVarcharType())
            .hiddenColumn("comment", createUnboundedVarcharType()) // non-standard
            .hiddenColumn("extra_info", createUnboundedVarcharType()) // non-standard
            .hiddenColumn("column_comment", createUnboundedVarcharType()) // MySQL compatible
            .build()),
    TABLES(table("tables")
            .column("table_catalog", createUnboundedVarcharType())
            .column("table_schema", createUnboundedVarcharType())
            .column("table_name", createUnboundedVarcharType())
            .column("table_type", createUnboundedVarcharType())
            .hiddenColumn("table_comment", createUnboundedVarcharType()) // MySQL compatible
            .build()),
    VIEWS(table("views")
            .column("table_catalog", createUnboundedVarcharType())
            .column("table_schema", createUnboundedVarcharType())
            .column("table_name", createUnboundedVarcharType())
            .column("view_definition", createUnboundedVarcharType())
            .build()),
    SCHEMATA(table("schemata")
            .column("catalog_name", createUnboundedVarcharType())
            .column("schema_name", createUnboundedVarcharType())
            .build()),
    TABLE_PRIVILEGES(table("table_privileges")
            .column("grantor", createUnboundedVarcharType())
            .column("grantor_type", createUnboundedVarcharType())
            .column("grantee", createUnboundedVarcharType())
            .column("grantee_type", createUnboundedVarcharType())
            .column("table_catalog", createUnboundedVarcharType())
            .column("table_schema", createUnboundedVarcharType())
            .column("table_name", createUnboundedVarcharType())
            .column("privilege_type", createUnboundedVarcharType())
            .column("is_grantable", createUnboundedVarcharType())
            .column("with_hierarchy", createUnboundedVarcharType())
            .build()),
    ROLES(table("roles")
            .column("role_name", createUnboundedVarcharType())
            .build()),
    APPLICABLE_ROLES(table("applicable_roles")
            .column("grantee", createUnboundedVarcharType())
            .column("grantee_type", createUnboundedVarcharType())
            .column("role_name", createUnboundedVarcharType())
            .column("is_grantable", createUnboundedVarcharType())
            .build()),
    ENABLED_ROLES(table("enabled_roles")
            .column("role_name", createUnboundedVarcharType())
            .build());

    public static final String INFORMATION_SCHEMA = "information_schema";

    private final ConnectorTableMetadata tableMetadata;

    InformationSchemaTable(ConnectorTableMetadata tableMetadata)
    {
        this.tableMetadata = requireNonNull(tableMetadata, "tableMetadata is null");
    }

    private static TableMetadataBuilder table(String tableName)
    {
        return tableMetadataBuilder(new SchemaTableName(INFORMATION_SCHEMA, tableName));
    }

    public static Optional<InformationSchemaTable> of(SchemaTableName schemaTableName)
    {
        if (!schemaTableName.getSchemaName().equals(INFORMATION_SCHEMA)) {
            return Optional.empty();
        }
        try {
            return Optional.of(valueOf(schemaTableName.getTableName().toUpperCase(ENGLISH)));
        }
        catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    public ConnectorTableMetadata getTableMetadata()
    {
        return tableMetadata;
    }

    public SchemaTableName getSchemaTableName()
    {
        return tableMetadata.getTable();
    }

    @JsonCreator
    public InformationSchemaTable fromStringValue(String value)
    {
        return valueOf(value);
    }

    @JsonValue
    public String toStringValue()
    {
        return name();
    }
}
