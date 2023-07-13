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
package io.trino.plugin.hive.avro;

import com.google.inject.Inject;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.memory.MemoryInputFile;
import io.trino.hive.formats.avro.AvroTypeException;
import io.trino.plugin.hive.AcidInfo;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePageSourceFactory;
import io.trino.plugin.hive.HiveTimestampPrecision;
import io.trino.plugin.hive.ReaderColumns;
import io.trino.plugin.hive.ReaderPageSource;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.fs.MonitoredInputFile;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.predicate.TupleDomain;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.util.internal.Accessor;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.AbstractCollection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.hive.HivePageSourceProvider.projectBaseColumns;
import static io.trino.plugin.hive.HiveSessionProperties.getTimestampPrecision;
import static io.trino.plugin.hive.HiveSessionProperties.isAvroNativeReaderEnabled;
import static io.trino.plugin.hive.ReaderPageSource.noProjectionAdaptation;
import static io.trino.plugin.hive.avro.AvroHiveFileUtils.wrapInUnionWithNull;
import static io.trino.plugin.hive.util.HiveClassNames.AVRO_SERDE_CLASS;
import static io.trino.plugin.hive.util.HiveUtil.getDeserializerClassName;
import static io.trino.plugin.hive.util.HiveUtil.splitError;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class AvroHivePageSourceFactory
        implements HivePageSourceFactory
{
    private static final DataSize BUFFER_SIZE = DataSize.of(8, DataSize.Unit.MEGABYTE);

    private final TrinoFileSystemFactory trinoFileSystemFactory;
    private final FileFormatDataSourceStats stats;

    @Inject
    public AvroHivePageSourceFactory(TrinoFileSystemFactory trinoFileSystemFactory, FileFormatDataSourceStats stats)
    {
        this.trinoFileSystemFactory = requireNonNull(trinoFileSystemFactory, "trinoFileSystemFactory is null");
        this.stats = requireNonNull(stats, "stats is null");
    }

    @Override
    public Optional<ReaderPageSource> createPageSource(
            Configuration configuration,
            ConnectorSession session,
            Location path,
            long start,
            long length,
            long estimatedFileSize,
            Properties schema,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            Optional<AcidInfo> acidInfo,
            OptionalInt bucketNumber,
            boolean originalFile,
            AcidTransaction transaction)
    {
        if (!isAvroNativeReaderEnabled(session)) {
            return Optional.empty();
        }
        else if (!AVRO_SERDE_CLASS.equals(getDeserializerClassName(schema))) {
            return Optional.empty();
        }
        checkArgument(acidInfo.isEmpty(), "Acid is not supported");

        List<HiveColumnHandle> projectedReaderColumns = columns;
        Optional<ReaderColumns> readerProjections = projectBaseColumns(columns);

        if (readerProjections.isPresent()) {
            projectedReaderColumns = readerProjections.get().get().stream()
                    .map(HiveColumnHandle.class::cast)
                    .collect(toImmutableList());
        }

        TrinoFileSystem trinoFileSystem = trinoFileSystemFactory.create(session.getIdentity());
        TrinoInputFile inputFile = new MonitoredInputFile(stats, trinoFileSystem.newInputFile(path));

        Schema tableSchema;
        try {
            tableSchema = AvroHiveFileUtils.determineSchemaOrThrowException(trinoFileSystem, schema);
        }
        catch (IOException | org.apache.avro.AvroTypeException e) {
            throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, "Unable to load or parse schema", e);
        }

        try {
            length = min(inputFile.length() - start, length);
            if (estimatedFileSize < BUFFER_SIZE.toBytes()) {
                try (TrinoInputStream input = inputFile.newStream()) {
                    byte[] data = input.readAllBytes();
                    inputFile = new MemoryInputFile(path, Slices.wrappedBuffer(data));
                }
            }
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (Exception e) {
            throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, splitError(e, path, start, length), e);
        }

        // Split may be empty now that the correct file size is known
        if (length <= 0) {
            return Optional.of(noProjectionAdaptation(new EmptyPageSource()));
        }

        Schema maskedSchema;
        try {
            maskedSchema = maskColumnsFromTableSchema(projectedReaderColumns, tableSchema);
        }
        catch (org.apache.avro.AvroTypeException e) {
            throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, "Avro type resolution error when initializing split from %s".formatted(path), e);
        }

        HiveTimestampPrecision hiveTimestampPrecision = getTimestampPrecision(session);
        if (maskedSchema.getFields().isEmpty()) {
            // no non-masked columns to select from partition schema
            // hack to return null rows with same total count as underlying data file
            // will error if UUID is same name as base column for underlying storage table but should never
            // return false data. If file data has f+uuid column in schema then resolution of read null from not null will fail.
            SchemaBuilder.FieldAssembler<Schema> nullSchema = SchemaBuilder.record("null_only").fields();
            for (int i = 0; i < Math.max(projectedReaderColumns.size(), 1); i++) {
                String notAColumnName = null;
                while (Objects.isNull(notAColumnName) || Objects.nonNull(tableSchema.getField(notAColumnName))) {
                    notAColumnName = "f" + UUID.randomUUID().toString().replace('-', '_');
                }
                nullSchema = nullSchema.name(notAColumnName).type(Schema.create(Schema.Type.NULL)).withDefault(null);
            }
            try {
                return Optional.of(noProjectionAdaptation(new AvroHivePageSource(inputFile, nullSchema.endRecord(), new HiveAvroTypeManager(hiveTimestampPrecision), start, length)));
            }
            catch (IOException e) {
                throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, e);
            }
            catch (AvroTypeException e) {
                throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, "Avro type resolution error when initializing split from %s".formatted(path), e);
            }
        }

        try {
            return Optional.of(new ReaderPageSource(new AvroHivePageSource(inputFile, maskedSchema, new HiveAvroTypeManager(hiveTimestampPrecision), start, length), readerProjections));
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, e);
        }
        catch (AvroTypeException e) {
            throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, "Avro type resolution error when initializing split from %s".formatted(path), e);
        }
    }

    private Schema maskColumnsFromTableSchema(List<HiveColumnHandle> columns, Schema tableSchema)
    {
        verify(tableSchema.getType() == Schema.Type.RECORD);
        Set<String> maskedColumns = columns.stream().map(HiveColumnHandle::getBaseColumnName).collect(LinkedHashSet::new, HashSet::add, AbstractCollection::addAll);

        SchemaBuilder.FieldAssembler<Schema> maskedSchema = SchemaBuilder.builder()
                .record(tableSchema.getName())
                .namespace(tableSchema.getNamespace())
                .fields();

        for (String columnName : maskedColumns) {
            Schema.Field field = tableSchema.getField(columnName);
            if (Objects.isNull(field)) {
                continue;
            }
            if (field.hasDefaultValue()) {
                try {
                    Object defaultObj = Accessor.defaultValue(field);
                    maskedSchema = maskedSchema
                            .name(field.name())
                            .aliases(field.aliases().toArray(String[]::new))
                            .doc(field.doc())
                            .type(field.schema())
                            .withDefault(defaultObj);
                }
                catch (org.apache.avro.AvroTypeException e) {
                    // in order to maintain backwards compatibility invalid defaults are mapped to null
                    // behavior defined by io.trino.tests.product.hive.TestAvroSchemaStrictness.testInvalidUnionDefaults
                    // solution is to make the field nullable and default-able to null. Any place default would be used, null will be
                    if (e.getMessage().contains("Invalid default")) {
                        maskedSchema = maskedSchema
                                .name(field.name())
                                .aliases(field.aliases().toArray(String[]::new))
                                .doc(field.doc())
                                .type(wrapInUnionWithNull(field.schema()))
                                .withDefault(null);
                    }
                    else {
                        throw e;
                    }
                }
            }
            else {
                maskedSchema = maskedSchema
                        .name(field.name())
                        .aliases(field.aliases().toArray(String[]::new))
                        .doc(field.doc())
                        .type(field.schema())
                        .noDefault();
            }
        }
        return maskedSchema.endRecord();
    }
}
