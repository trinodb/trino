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
package io.trino.plugin.iceberg.functions.tablechanges;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.IcebergPageSourceProvider;
import io.trino.plugin.iceberg.PartitionData;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.function.table.TableFunctionProcessorState;
import io.trino.spi.function.table.TableFunctionSplitProcessor;
import io.trino.spi.predicate.TupleDomain;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.mapping.NameMappingParser;

import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.iceberg.IcebergColumnHandle.DATA_CHANGE_ORDINAL_ID;
import static io.trino.plugin.iceberg.IcebergColumnHandle.DATA_CHANGE_TIMESTAMP_ID;
import static io.trino.plugin.iceberg.IcebergColumnHandle.DATA_CHANGE_TYPE_ID;
import static io.trino.plugin.iceberg.IcebergColumnHandle.DATA_CHANGE_VERSION_ID;
import static io.trino.spi.function.table.TableFunctionProcessorState.Finished.FINISHED;
import static io.trino.spi.function.table.TableFunctionProcessorState.Processed.produced;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;

public class TableChangesFunctionProcessor
        implements TableFunctionSplitProcessor
{
    private static final Page EMPTY_PAGE = new Page(0);

    private final ConnectorPageSource pageSource;
    private final int[] delegateColumnMap;
    private final Optional<Integer> changeTypeIndex;
    private final Block changeTypeValue;
    private final Optional<Integer> changeVersionIndex;
    private final Block changeVersionValue;
    private final Optional<Integer> changeTimestampIndex;
    private final Block changeTimestampValue;
    private final Optional<Integer> changeOrdinalIndex;
    private final Block changeOrdinalValue;

    public TableChangesFunctionProcessor(
            ConnectorSession session,
            TableChangesFunctionHandle functionHandle,
            TableChangesSplit split,
            IcebergPageSourceProvider icebergPageSourceProvider)
    {
        requireNonNull(session, "session is null");
        requireNonNull(functionHandle, "functionHandle is null");
        requireNonNull(split, "split is null");
        requireNonNull(icebergPageSourceProvider, "icebergPageSourceProvider is null");

        Schema tableSchema = SchemaParser.fromJson(functionHandle.tableSchemaJson());
        PartitionSpec partitionSpec = PartitionSpecParser.fromJson(tableSchema, split.partitionSpecJson());
        org.apache.iceberg.types.Type[] partitionColumnTypes = partitionSpec.fields().stream()
                .map(field -> field.transform().getResultType(tableSchema.findType(field.sourceId())))
                .toArray(org.apache.iceberg.types.Type[]::new);

        int delegateColumnIndex = 0;
        int[] delegateColumnMap = new int[functionHandle.columns().size()];
        Optional<Integer> changeTypeIndex = Optional.empty();
        Optional<Integer> changeVersionIndex = Optional.empty();
        Optional<Integer> changeTimestampIndex = Optional.empty();
        Optional<Integer> changeOrdinalIndex = Optional.empty();
        for (int columnIndex = 0; columnIndex < functionHandle.columns().size(); columnIndex++) {
            IcebergColumnHandle column = functionHandle.columns().get(columnIndex);
            if (column.getId() == DATA_CHANGE_TYPE_ID) {
                changeTypeIndex = Optional.of(columnIndex);
                delegateColumnMap[columnIndex] = -1;
            }
            else if (column.getId() == DATA_CHANGE_VERSION_ID) {
                changeVersionIndex = Optional.of(columnIndex);
                delegateColumnMap[columnIndex] = -1;
            }
            else if (column.getId() == DATA_CHANGE_TIMESTAMP_ID) {
                changeTimestampIndex = Optional.of(columnIndex);
                delegateColumnMap[columnIndex] = -1;
            }
            else if (column.getId() == DATA_CHANGE_ORDINAL_ID) {
                changeOrdinalIndex = Optional.of(columnIndex);
                delegateColumnMap[columnIndex] = -1;
            }
            else {
                delegateColumnMap[columnIndex] = delegateColumnIndex;
                delegateColumnIndex++;
            }
        }

        this.pageSource = icebergPageSourceProvider.createPageSource(
                session,
                functionHandle.columns(),
                tableSchema,
                partitionSpec,
                PartitionData.fromJson(split.partitionDataJson(), partitionColumnTypes),
                ImmutableList.of(),
                DynamicFilter.EMPTY,
                TupleDomain.all(),
                TupleDomain.all(),
                split.path(),
                split.start(),
                split.length(),
                split.fileSize(),
                split.fileRecordCount(),
                split.partitionDataJson(),
                split.fileFormat(),
                split.fileIoProperties(),
                functionHandle.nameMappingJson().map(NameMappingParser::fromJson));
        this.delegateColumnMap = delegateColumnMap;

        this.changeTypeIndex = changeTypeIndex;
        this.changeTypeValue = nativeValueToBlock(createUnboundedVarcharType(), utf8Slice(split.changeType().getTableValue()));

        this.changeVersionIndex = changeVersionIndex;
        this.changeVersionValue = nativeValueToBlock(BIGINT, split.snapshotId());

        this.changeTimestampIndex = changeTimestampIndex;
        this.changeTimestampValue = nativeValueToBlock(TIMESTAMP_TZ_MILLIS, split.snapshotTimestamp());

        this.changeOrdinalIndex = changeOrdinalIndex;
        this.changeOrdinalValue = nativeValueToBlock(INTEGER, (long) split.changeOrdinal());
    }

    @Override
    public TableFunctionProcessorState process()
    {
        if (pageSource.isFinished()) {
            return FINISHED;
        }

        Page dataPage = pageSource.getNextPage();
        if (dataPage == null) {
            return TableFunctionProcessorState.Processed.produced(EMPTY_PAGE);
        }

        Block[] blocks = new Block[delegateColumnMap.length];
        for (int targetChannel = 0; targetChannel < delegateColumnMap.length; targetChannel++) {
            int delegateIndex = delegateColumnMap[targetChannel];
            if (delegateIndex != -1) {
                blocks[targetChannel] = dataPage.getBlock(delegateIndex);
            }
        }

        changeTypeIndex.ifPresent(columnChannel ->
                blocks[columnChannel] = RunLengthEncodedBlock.create(changeTypeValue, dataPage.getPositionCount()));
        changeVersionIndex.ifPresent(columnChannel ->
                blocks[columnChannel] = RunLengthEncodedBlock.create(changeVersionValue, dataPage.getPositionCount()));
        changeTimestampIndex.ifPresent(columnChannel ->
                blocks[columnChannel] = RunLengthEncodedBlock.create(changeTimestampValue, dataPage.getPositionCount()));
        changeOrdinalIndex.ifPresent(columnChannel ->
                blocks[columnChannel] = RunLengthEncodedBlock.create(changeOrdinalValue, dataPage.getPositionCount()));

        return produced(new Page(dataPage.getPositionCount(), blocks));
    }
}
