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
package io.trino.connector.system;

import com.google.common.collect.ImmutableList;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.InMemoryRecordSet.Builder;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignatureParameter;
import io.trino.spi.type.VarcharType;
import io.trino.transaction.TransactionInfo;
import io.trino.transaction.TransactionManager;
import org.joda.time.DateTime;

import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.trino.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.trino.spi.connector.SystemTable.Distribution.SINGLE_COORDINATOR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.StandardTypes.ARRAY;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;

public class TransactionsSystemTable
        implements SystemTable
{
    public static final SchemaTableName TRANSACTIONS_TABLE_NAME = new SchemaTableName("runtime", "transactions");

    private final ConnectorTableMetadata transactionsTable;
    private final TransactionManager transactionManager;

    @Inject
    public TransactionsSystemTable(TypeManager typeManager, TransactionManager transactionManager)
    {
        this.transactionsTable = tableMetadataBuilder(TRANSACTIONS_TABLE_NAME)
                .column("transaction_id", createUnboundedVarcharType())
                .column("isolation_level", createUnboundedVarcharType())
                .column("read_only", BOOLEAN)
                .column("auto_commit_context", BOOLEAN)
                .column("create_time", TIMESTAMP_TZ_MILLIS)
                .column("idle_time_secs", BIGINT)
                .column("written_catalog", createUnboundedVarcharType())
                .column("catalogs", typeManager.getParameterizedType(ARRAY, ImmutableList.of(TypeSignatureParameter.typeParameter(createUnboundedVarcharType().getTypeSignature()))))
                .build();
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
    }

    @Override
    public Distribution getDistribution()
    {
        return SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return transactionsTable;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        Builder table = InMemoryRecordSet.builder(transactionsTable);
        for (TransactionInfo info : transactionManager.getAllTransactionInfos()) {
            table.addRow(
                    info.getTransactionId().toString(),
                    info.getIsolationLevel().toString(),
                    info.isReadOnly(),
                    info.isAutoCommitContext(),
                    toTimestampWithTimeZoneMillis(info.getCreateTime()),
                    (long) info.getIdleTime().getValue(TimeUnit.SECONDS),
                    info.getWrittenCatalogName().orElse(null),
                    createStringsBlock(info.getCatalogNames()));
        }
        return table.build().cursor();
    }

    private static Block createStringsBlock(List<String> values)
    {
        VarcharType varchar = createUnboundedVarcharType();
        BlockBuilder builder = varchar.createBlockBuilder(null, values.size());
        for (String value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                varchar.writeString(builder, value);
            }
        }
        return builder.build();
    }

    private static Long toTimestampWithTimeZoneMillis(DateTime dateTime)
    {
        // dateTime.getZone() is the server zone, should be of no interest to the user
        return packDateTimeWithZone(dateTime.getMillis(), UTC_KEY);
    }
}
