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

public class AttributeJdbcTable
        extends JdbcTable
{
    public static final SchemaTableName NAME = new SchemaTableName("jdbc", "attributes");

    public static final ConnectorTableMetadata METADATA = tableMetadataBuilder(NAME)
            .column("TYPE_CAT", VARCHAR)
            .column("TYPE_SCHEM", VARCHAR)
            .column("TYPE_NAME", VARCHAR)
            .column("ATTR_NAME", VARCHAR)
            .column("DATA_TYPE", BIGINT)
            .column("ATTR_TYPE_NAME", VARCHAR)
            .column("ATTR_SIZE", BIGINT)
            .column("DECIMAL_DIGITS", BIGINT)
            .column("NUM_PREC_RADIX", BIGINT)
            .column("NULLABLE", BIGINT)
            .column("REMARKS", VARCHAR)
            .column("ATTR_DEF", VARCHAR)
            .column("SQL_DATA_TYPE", BIGINT)
            .column("SQL_DATETIME_SUB", BIGINT)
            .column("CHAR_OCTET_LENGTH", BIGINT)
            .column("ORDINAL_POSITION", BIGINT)
            .column("IS_NULLABLE", VARCHAR)
            .column("SCOPE_CATALOG", VARCHAR)
            .column("SCOPE_SCHEMA", VARCHAR)
            .column("SCOPE_TABLE", VARCHAR)
            .column("SOURCE_DATA_TYPE", BIGINT)
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
