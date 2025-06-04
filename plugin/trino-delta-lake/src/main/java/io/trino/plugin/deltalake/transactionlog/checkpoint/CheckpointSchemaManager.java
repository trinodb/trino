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
import io.trino.plugin.deltalake.DeltaHiveTypeTranslator;
import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.plugin.deltalake.DeltaLakeColumnMetadata;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.extractPartitionColumns;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.extractSchema;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.isDeletionVectorEnabled;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogAccess.columnsWithStats;
import static io.trino.plugin.hive.util.HiveTypeUtil.getTypeSignature;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class CheckpointSchemaManager
{
    private final TypeManager typeManager;

    private static final RowType DELETION_VECTORS_TYPE = RowType.from(ImmutableList.<RowType.Field>builder()
            .add(RowType.field("storageType", VARCHAR))
            .add(RowType.field("pathOrInlineDv", VARCHAR))
            .add(RowType.field("offset", INTEGER))
            .add(RowType.field("sizeInBytes", INTEGER))
            .add(RowType.field("cardinality", BIGINT))
            .build());

    private static final RowType TXN_ENTRY_TYPE = RowType.from(ImmutableList.of(
            RowType.field("appId", VARCHAR),
            RowType.field("version", BIGINT),
            RowType.field("lastUpdated", BIGINT)));

    private final RowType metadataEntryType;
    private final RowType removeEntryType;
    private final RowType sidecarEntryType;
    private final ArrayType stringList;

    @Inject
    public CheckpointSchemaManager(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");

        stringList = (ArrayType) this.typeManager.getType(TypeSignature.arrayType(VARCHAR.getTypeSignature()));
        MapType stringMap = (MapType) this.typeManager.getType(TypeSignature.mapType(VARCHAR.getTypeSignature(), VARCHAR.getTypeSignature()));

        metadataEntryType = RowType.from(ImmutableList.of(
                RowType.field("id", VARCHAR),
                RowType.field("name", VARCHAR),
                RowType.field("description", VARCHAR),
                RowType.field("format", RowType.from(ImmutableList.of(
                        RowType.field("provider", VARCHAR),
                        RowType.field("options", stringMap)))),
                RowType.field("schemaString", VARCHAR),
                RowType.field("partitionColumns", stringList),
                RowType.field("configuration", stringMap),
                RowType.field("createdTime", BIGINT)));

        removeEntryType = RowType.from(ImmutableList.of(
                RowType.field("path", VARCHAR),
                RowType.field("partitionValues", stringMap),
                RowType.field("deletionTimestamp", BIGINT),
                RowType.field("dataChange", BOOLEAN)));

        sidecarEntryType = RowType.from(ImmutableList.<RowType.Field>builder()
                .add(RowType.field("path", VARCHAR))
                .add(RowType.field("sizeInBytes", BIGINT))
                .add(RowType.field("modificationTime", BIGINT))
                .add(RowType.field("tags", stringMap))
                .build());
    }

    public RowType getMetadataEntryType()
    {
        return metadataEntryType;
    }

    public RowType getAddEntryType(
            MetadataEntry metadataEntry,
            ProtocolEntry protocolEntry,
            Predicate<String> addStatsMinMaxColumnFilter,
            boolean requireWriteStatsAsJson,
            boolean requireWriteStatsAsStruct,
            boolean usePartitionValues)
    {
        List<DeltaLakeColumnMetadata> allColumns = extractSchema(metadataEntry, protocolEntry, typeManager);
        List<DeltaLakeColumnMetadata> minMaxColumns = columnsWithStats(metadataEntry, protocolEntry, typeManager);
        minMaxColumns = minMaxColumns.stream()
                .filter(column -> addStatsMinMaxColumnFilter.test(column.name()))
                .collect(toImmutableList());
        boolean deletionVectorEnabled = isDeletionVectorEnabled(metadataEntry, protocolEntry);

        ImmutableList.Builder<RowType.Field> minMaxFields = ImmutableList.builder();
        for (DeltaLakeColumnMetadata dataColumn : minMaxColumns) {
            Type type = dataColumn.physicalColumnType();
            if (type instanceof TimestampWithTimeZoneType) {
                minMaxFields.add(RowType.field(dataColumn.physicalName(), TIMESTAMP_MILLIS));
            }
            else {
                minMaxFields.add(RowType.field(dataColumn.physicalName(), type));
            }
        }

        ImmutableList.Builder<RowType.Field> statsColumns = ImmutableList.builder();
        statsColumns.add(RowType.field("numRecords", BIGINT));

        List<RowType.Field> minMax = minMaxFields.build();
        if (!minMax.isEmpty()) {
            RowType minMaxType = RowType.from(minMax);
            statsColumns.add(RowType.field("minValues", minMaxType));
            statsColumns.add(RowType.field("maxValues", minMaxType));
        }

        statsColumns.add(RowType.field(
                "nullCount",
                RowType.from(allColumns.stream().map(column -> buildNullCountType(Optional.of(column.physicalName()), column.physicalColumnType())).collect(toImmutableList()))));

        MapType stringMap = (MapType) typeManager.getType(TypeSignature.mapType(VARCHAR.getTypeSignature(), VARCHAR.getTypeSignature()));
        ImmutableList.Builder<RowType.Field> addFields = ImmutableList.builder();
        addFields.add(RowType.field("path", VARCHAR));
        if (usePartitionValues) {
            addFields.add(RowType.field("partitionValues", stringMap));
        }
        addFields.add(RowType.field("size", BIGINT));
        addFields.add(RowType.field("modificationTime", BIGINT));
        addFields.add(RowType.field("dataChange", BOOLEAN));
        if (requireWriteStatsAsJson) {
            addFields.add(RowType.field("stats", VARCHAR));
        }
        if (requireWriteStatsAsStruct) {
            List<DeltaLakeColumnHandle> partitionColumns = extractPartitionColumns(metadataEntry, protocolEntry, typeManager);
            if (!partitionColumns.isEmpty()) {
                List<RowType.Field> partitionValuesParsed = partitionColumns.stream()
                        .map(column -> RowType.field(column.basePhysicalColumnName(), typeManager.getType(getTypeSignature(DeltaHiveTypeTranslator.toHiveType(column.type())))))
                        .collect(toImmutableList());
                addFields.add(RowType.field("partitionValues_parsed", RowType.from(partitionValuesParsed)));
            }
            addFields.add(RowType.field("stats_parsed", RowType.from(statsColumns.build())));
        }
        addFields.add(RowType.field("tags", stringMap));
        if (deletionVectorEnabled) {
            addFields.add(RowType.field("deletionVector", DELETION_VECTORS_TYPE));
        }

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
        return new RowType.Field(columnName, BIGINT);
    }

    public RowType getRemoveEntryType()
    {
        return removeEntryType;
    }

    public RowType getTxnEntryType()
    {
        return TXN_ENTRY_TYPE;
    }

    public RowType getProtocolEntryType(boolean requireReaderFeatures, boolean requireWriterFeatures)
    {
        ImmutableList.Builder<RowType.Field> fields = ImmutableList.builder();
        fields.add(RowType.field("minReaderVersion", INTEGER));
        fields.add(RowType.field("minWriterVersion", INTEGER));
        if (requireReaderFeatures) {
            fields.add(RowType.field("readerFeatures", stringList));
        }
        if (requireWriterFeatures) {
            fields.add(RowType.field("writerFeatures", stringList));
        }
        return RowType.from(fields.build());
    }

    public RowType getSidecarEntryType()
    {
        return sidecarEntryType;
    }
}
