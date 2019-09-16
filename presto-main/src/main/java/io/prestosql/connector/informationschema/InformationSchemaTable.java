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
package io.prestosql.connector.informationschema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;

import java.util.Optional;

import static io.prestosql.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.prestosql.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Locale.ENGLISH;

public enum InformationSchemaTable
{
    COLUMNS("columns"),
    TABLES("tables"),
    VIEWS("views"),
    SCHEMATA("schemata"),
    TABLE_PRIVILEGES("table_privileges"),
    ROLES("roles"),
    APPLICABLE_ROLES("applicable_roles"),
    ENABLED_ROLES("enabled_roles");

    public static final String INFORMATION_SCHEMA = "information_schema";

    private final SchemaTableName schemaTableName;
    private final ConnectorTableMetadata tableMetadata;

    InformationSchemaTable(String tableName)
    {
        schemaTableName = new SchemaTableName(INFORMATION_SCHEMA, tableName);
        if (tableName.equals("columns")) {
            tableMetadata = tableMetadataBuilder(schemaTableName)
                    .column("table_catalog", createUnboundedVarcharType())
                    .column("table_schema", createUnboundedVarcharType())
                    .column("table_name", createUnboundedVarcharType())
                    .column("column_name", createUnboundedVarcharType())
                    .column("ordinal_position", BIGINT)
                    .column("column_default", createUnboundedVarcharType())
                    .column("is_nullable", createUnboundedVarcharType())
                    .column("data_type", createUnboundedVarcharType())
                    .column("comment", createUnboundedVarcharType())
                    .column("extra_info", createUnboundedVarcharType())
                    .hiddenColumn("column_comment", createUnboundedVarcharType()) // MySQL compatible
                    .build();
            return;
        }
        if (tableName.equals("tables")) {
            tableMetadata = tableMetadataBuilder(schemaTableName)
                    .column("table_catalog", createUnboundedVarcharType())
                    .column("table_schema", createUnboundedVarcharType())
                    .column("table_name", createUnboundedVarcharType())
                    .column("table_type", createUnboundedVarcharType())
                    .hiddenColumn("table_comment", createUnboundedVarcharType()) // MySQL compatible
                    .build();
            return;
        }
        if (tableName.equals("views")) {
            tableMetadata = tableMetadataBuilder(schemaTableName)
                    .column("table_catalog", createUnboundedVarcharType())
                    .column("table_schema", createUnboundedVarcharType())
                    .column("table_name", createUnboundedVarcharType())
                    .column("view_definition", createUnboundedVarcharType())
                    .build();
            return;
        }
        if (tableName.equals("schemata")) {
            tableMetadata = tableMetadataBuilder(schemaTableName)
                    .column("catalog_name", createUnboundedVarcharType())
                    .column("schema_name", createUnboundedVarcharType())
                    .build();
            return;
        }
        if (tableName.equals("table_privileges")) {
            tableMetadata = tableMetadataBuilder(schemaTableName)
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
                    .build();
            return;
        }
        if (tableName.equals("roles")) {
            tableMetadata = tableMetadataBuilder(schemaTableName)
                    .column("role_name", createUnboundedVarcharType())
                    .build();
            return;
        }
        if (tableName.equals("applicable_roles")) {
            tableMetadata = tableMetadataBuilder(schemaTableName)
                    .column("grantee", createUnboundedVarcharType())
                    .column("grantee_type", createUnboundedVarcharType())
                    .column("role_name", createUnboundedVarcharType())
                    .column("is_grantable", createUnboundedVarcharType())
                    .build();
            return;
        }
        if (tableName.equals("enabled_roles")) {
            tableMetadata = tableMetadataBuilder(schemaTableName)
                    .column("role_name", createUnboundedVarcharType())
                    .build();
            return;
        }
        throw new PrestoException(INVALID_ARGUMENTS, "Invalid table name");
    }

    public static Optional<InformationSchemaTable> of(SchemaTableName schemaTableName)
    {
        if (!schemaTableName.getSchemaName().equals(INFORMATION_SCHEMA)) {
            return Optional.empty();
        }
        try {
            return Optional.of(InformationSchemaTable.valueOf(schemaTableName.getTableName().toUpperCase(ENGLISH)));
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
        return schemaTableName;
    }

    @JsonCreator
    public InformationSchemaTable fromStringValue(String value)
    {
        return InformationSchemaTable.valueOf(value);
    }

    @JsonValue
    public String toStringValue()
    {
        return name();
    }
}
