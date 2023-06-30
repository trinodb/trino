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
package io.trino.plugin.deltalake.transactionlog.checkpoint;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.deltalake.DeltaLakeColumnMetadata;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarcharType;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.extractSchema;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogAccess.columnsWithStats;
import static java.util.Objects.requireNonNull;

public class CheckpointSchemaManager
{
    private final TypeManager typeManager;

    private static final RowType TXN_ENTRY_TYPE = RowType.from(ImmutableList.of(
            RowType.field("appId", VarcharType.createUnboundedVarcharType()),
            RowType.field("version", BigintType.BIGINT),
            RowType.field("lastUpdated", BigintType.BIGINT)));

    private static final RowType REMOVE_ENTRY_TYPE = RowType.from(ImmutableList.of(
            RowType.field("path", VarcharType.createUnboundedVarcharType()),
            RowType.field("deletionTimestamp", BigintType.BIGINT),
            RowType.field("dataChange", BooleanType.BOOLEAN)));

    private final RowType metadataEntryType;
    private final RowType commitInfoEntryType;
    private final ArrayType stringList;

    @Inject
    public CheckpointSchemaManager(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");

        stringList = (ArrayType) this.typeManager.getType(TypeSignature.arrayType(VarcharType.VARCHAR.getTypeSignature()));
        MapType stringMap = (MapType) this.typeManager.getType(TypeSignature.mapType(VarcharType.VARCHAR.getTypeSignature(), VarcharType.VARCHAR.getTypeSignature()));

        metadataEntryType = RowType.from(ImmutableList.of(
                RowType.field("id", VarcharType.createUnboundedVarcharType()),
                RowType.field("name", VarcharType.createUnboundedVarcharType()),
                RowType.field("description", VarcharType.createUnboundedVarcharType()),
                RowType.field("format", RowType.from(ImmutableList.of(
                        RowType.field("provider", VarcharType.createUnboundedVarcharType()),
                        RowType.field("options", stringMap)))),
                RowType.field("schemaString", VarcharType.createUnboundedVarcharType()),
                RowType.field("partitionColumns", stringList),
                RowType.field("configuration", stringMap),
                RowType.field("createdTime", BigintType.BIGINT)));

        commitInfoEntryType = RowType.from(ImmutableList.of(
                RowType.field("version", BigintType.BIGINT),
                RowType.field("timestamp", TimestampType.TIMESTAMP_MILLIS),
                RowType.field("userId", VarcharType.createUnboundedVarcharType()),
                RowType.field("userName", VarcharType.createUnboundedVarcharType()),
                RowType.field("operation", VarcharType.createUnboundedVarcharType()),
                RowType.field("operationParameters", stringMap),
                RowType.field("job", RowType.from(ImmutableList.of(
                        RowType.field("jobId", VarcharType.createUnboundedVarcharType()),
                        RowType.field("jobName", VarcharType.createUnboundedVarcharType()),
                        RowType.field("runId", VarcharType.createUnboundedVarcharType()),
                        RowType.field("jobOwnerId", VarcharType.createUnboundedVarcharType()),
                        RowType.field("triggerType", VarcharType.createUnboundedVarcharType())))),
                RowType.field("notebook", RowType.from(
                        ImmutableList.of(RowType.field("notebookId", VarcharType.createUnboundedVarcharType())))),
                RowType.field("clusterId", VarcharType.createUnboundedVarcharType()),
                RowType.field("readVersion", BigintType.BIGINT),
                RowType.field("isolationLevel", VarcharType.createUnboundedVarcharType()),
                RowType.field("isBlindAppend", BooleanType.BOOLEAN)));
    }

    public RowType getMetadataEntryType()
    {
        return metadataEntryType;
    }

    public RowType getAddEntryType(MetadataEntry metadataEntry, boolean requireWriteStatsAsJson, boolean requireWriteStatsAsStruct)
    {
        List<DeltaLakeColumnMetadata> allColumns = extractSchema(metadataEntry, typeManager);
        List<DeltaLakeColumnMetadata> minMaxColumns = columnsWithStats(metadataEntry, typeManager);

        ImmutableList.Builder<RowType.Field> minMaxFields = ImmutableList.builder();
        for (DeltaLakeColumnMetadata dataColumn : minMaxColumns) {
            Type type = dataColumn.getPhysicalColumnType();
            if (type instanceof TimestampWithTimeZoneType) {
                minMaxFields.add(RowType.field(dataColumn.getPhysicalName(), TimestampType.TIMESTAMP_MILLIS));
            }
            else {
                minMaxFields.add(RowType.field(dataColumn.getPhysicalName(), type));
            }
        }

        ImmutableList.Builder<RowType.Field> statsColumns = ImmutableList.builder();
        statsColumns.add(RowType.field("numRecords", BigintType.BIGINT));

        List<RowType.Field> minMax = minMaxFields.build();
        if (!minMax.isEmpty()) {
            RowType minMaxType = RowType.from(minMax);
            statsColumns.add(RowType.field("minValues", minMaxType));
            statsColumns.add(RowType.field("maxValues", minMaxType));
        }

        statsColumns.add(RowType.field(
                "nullCount",
                RowType.from(allColumns.stream().map(column -> buildNullCountType(Optional.of(column.getPhysicalName()), column.getPhysicalColumnType())).collect(toImmutableList()))));

        MapType stringMap = (MapType) typeManager.getType(TypeSignature.mapType(VarcharType.VARCHAR.getTypeSignature(), VarcharType.VARCHAR.getTypeSignature()));
        ImmutableList.Builder<RowType.Field> addFields = ImmutableList.builder();
        addFields.add(RowType.field("path", VarcharType.createUnboundedVarcharType()));
        addFields.add(RowType.field("partitionValues", stringMap));
        addFields.add(RowType.field("size", BigintType.BIGINT));
        addFields.add(RowType.field("modificationTime", BigintType.BIGINT));
        addFields.add(RowType.field("dataChange", BooleanType.BOOLEAN));
        if (requireWriteStatsAsJson) {
            addFields.add(RowType.field("stats", VarcharType.createUnboundedVarcharType()));
        }
        if (requireWriteStatsAsStruct) {
            addFields.add(RowType.field("stats_parsed", RowType.from(statsColumns.build())));
        }
        addFields.add(RowType.field("tags", stringMap));

        return RowType.from(addFields.build());
    }

    private static RowType.Field buildNullCountType(Optional<String> columnName, Type columnType)
    {
        if (columnType instanceof RowType rowType) {
            RowType rowTypeFromFields = RowType.from(
                    rowType.getFields().stream()
                            .map(field -> buildNullCountType(field.getName(), field.getType()))
                            .collect(toImmutableList()));
            return new RowType.Field(columnName, rowTypeFromFields);
        }
        return new RowType.Field(columnName, BigintType.BIGINT);
    }

    public RowType getRemoveEntryType()
    {
        return REMOVE_ENTRY_TYPE;
    }

    public RowType getTxnEntryType()
    {
        return TXN_ENTRY_TYPE;
    }

    public RowType getProtocolEntryType(boolean requireReaderFeatures, boolean requireWriterFeatures)
    {
        ImmutableList.Builder<RowType.Field> fields = ImmutableList.builder();
        fields.add(RowType.field("minReaderVersion", IntegerType.INTEGER));
        fields.add(RowType.field("minWriterVersion", IntegerType.INTEGER));
        if (requireReaderFeatures) {
            fields.add(RowType.field("readerFeatures", stringList));
        }
        if (requireWriterFeatures) {
            fields.add(RowType.field("writerFeatures", stringList));
        }
        return RowType.from(fields.build());
    }

    public RowType getCommitInfoEntryType()
    {
        return commitInfoEntryType;
    }}
