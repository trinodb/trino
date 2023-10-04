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
package io.trino.connector.system.jdbc;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import static io.trino.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class ProcedureColumnJdbcTable
        extends JdbcTable
{
    public static final SchemaTableName NAME = new SchemaTableName("jdbc", "procedure_columns");

    public static final ConnectorTableMetadata METADATA = tableMetadataBuilder(NAME)
            .column("procedure_cat", VARCHAR)
            .column("procedure_schem", VARCHAR)
            .column("procedure_name", VARCHAR)
            .column("column_name", VARCHAR)
            .column("column_type", BIGINT)
            .column("data_type", BIGINT)
            .column("type_name", VARCHAR)
            .column("precision", BIGINT)
            .column("length", BIGINT)
            .column("scale", BIGINT)
            .column("radix", BIGINT)
            .column("nullable", BIGINT)
            .column("remarks", VARCHAR)
            .column("column_def", VARCHAR)
            .column("sql_data_type", BIGINT)
            .column("sql_datetime_sub", BIGINT)
            .column("char_octet_length", BIGINT)
            .column("ordinal_position", BIGINT)
            .column("is_nullable", VARCHAR)
            .column("specific_name", VARCHAR)
            .build();

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return METADATA;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        return InMemoryRecordSet.builder(METADATA).build().cursor();
    }
}
