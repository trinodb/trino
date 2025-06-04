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
import io.trino.hive.formats.avro.HiveAvroTypeBlockHandler;
import io.trino.plugin.hive.AcidInfo;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePageSourceFactory;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.predicate.TupleDomain;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.util.internal.Accessor;

import java.io.IOException;
import java.util.AbstractCollection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.hive.formats.HiveClassNames.AVRO_SERDE_CLASS;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.hive.HivePageSourceProvider.projectColumnDereferences;
import static io.trino.plugin.hive.HiveSessionProperties.getTimestampPrecision;
import static io.trino.plugin.hive.avro.AvroHiveFileUtils.getCanonicalToGivenFieldName;
import static io.trino.plugin.hive.avro.AvroHiveFileUtils.wrapInUnionWithNull;
import static io.trino.plugin.hive.util.HiveUtil.splitError;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class AvroPageSourceFactory
        implements HivePageSourceFactory
{
    private static final DataSize BUFFER_SIZE = DataSize.of(8, DataSize.Unit.MEGABYTE);

    private final TrinoFileSystemFactory trinoFileSystemFactory;

    @Inject
    public AvroPageSourceFactory(TrinoFileSystemFactory trinoFileSystemFactory)
    {
        this.trinoFileSystemFactory = requireNonNull(trinoFileSystemFactory, "trinoFileSystemFactory is null");
    }

    @Override
    public Optional<ConnectorPageSource> createPageSource(
            ConnectorSession session,
            Location path,
            long start,
            long length,
            long estimatedFileSize,
            long fileModifiedTime,
            io.trino.plugin.hive.Schema schema,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            Optional<AcidInfo> acidInfo,
            OptionalInt bucketNumber,
            boolean originalFile,
            AcidTransaction transaction)
    {
        if (!AVRO_SERDE_CLASS.equals(schema.serializationLibraryName())) {
            return Optional.empty();
        }
        checkArgument(acidInfo.isEmpty(), "Acid is not supported");

        TrinoFileSystem trinoFileSystem = trinoFileSystemFactory.create(session);
        TrinoInputFile inputFile = cacheInputIfSmall(path, start, length, estimatedFileSize, trinoFileSystem);

        long actualSplitSize;
        try {
            actualSplitSize = min(inputFile.length() - start, length);
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, splitError(e, path, start, length), e);
        }

        // Split may be empty now that the correct file size is known
        if (actualSplitSize <= 0) {
            return Optional.of(new EmptyPageSource());
        }

        return Optional.of(projectColumnDereferences(columns, baseColumns -> createPageSource(session, trinoFileSystem, inputFile, start, actualSplitSize, schema, baseColumns)));
    }

    private static AvroPageSource createPageSource(
            ConnectorSession session,
            TrinoFileSystem trinoFileSystem,
            TrinoInputFile inputFile,
            long start,
            long length,
            io.trino.plugin.hive.Schema schema,
            List<HiveColumnHandle> columns)
    {
        verify(columns.stream().allMatch(HiveColumnHandle::isBaseColumn), "All columns must be base columns");
        Schema tableSchema;
        try {
            tableSchema = AvroHiveFileUtils.determineSchemaOrThrowException(trinoFileSystem, schema.serdeProperties());
        }
        catch (IOException | org.apache.avro.AvroTypeException e) {
            throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, "Unable to load or parse schema", e);
        }

        Schema maskedSchema;
        try {
            maskedSchema = maskColumnsFromTableSchema(columns, tableSchema);
        }
        catch (org.apache.avro.AvroTypeException e) {
            throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, "Avro type resolution error when initializing split from %s".formatted(inputFile.location()), e);
        }

        int hiveTimestampPrecision = getTimestampPrecision(session).getPrecision();
        if (maskedSchema.getFields().isEmpty()) {
            // No non-masked columns to select from partition schema.
            // Hack to return null rows with the same total count as the underlying data file.
            // This will error if UUID is the same name as base column for underlying storage table, but this should never
            // return false data. If file data has f+uuid column in schema, then the resolution of read null from not null will fail.
            SchemaBuilder.FieldAssembler<Schema> nullSchema = SchemaBuilder.record("null_only").fields();
            for (int i = 0; i < Math.max(columns.size(), 1); i++) {
                String notAColumnName = null;
                while (Objects.isNull(notAColumnName) || Objects.nonNull(tableSchema.getField(notAColumnName))) {
                    notAColumnName = "f" + UUID.randomUUID().toString().replace('-', '_');
                }
                nullSchema = nullSchema.name(notAColumnName).type(Schema.create(Schema.Type.NULL)).withDefault(null);
            }
            try {
                return new AvroPageSource(inputFile, nullSchema.endRecord(), new HiveAvroTypeBlockHandler(createTimestampType(hiveTimestampPrecision)), start, length);
            }
            catch (IOException e) {
                throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, e);
            }
            catch (AvroTypeException e) {
                throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, "Avro type resolution error when initializing split from %s".formatted(inputFile.location()), e);
            }
        }

        try {
            return new AvroPageSource(inputFile, maskedSchema, new HiveAvroTypeBlockHandler(createTimestampType(hiveTimestampPrecision)), start, length);
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, e);
        }
        catch (AvroTypeException e) {
            throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, "Avro type resolution error when initializing split from %s".formatted(inputFile.location()), e);
        }
    }

    private static Schema maskColumnsFromTableSchema(List<HiveColumnHandle> columns, Schema tableSchema)
    {
        verify(tableSchema.getType() == Schema.Type.RECORD);
        Set<String> maskedColumns = columns.stream().map(HiveColumnHandle::getBaseColumnName).collect(LinkedHashSet::new, HashSet::add, AbstractCollection::addAll);

        SchemaBuilder.FieldAssembler<Schema> maskedSchema = SchemaBuilder.builder()
                .record(tableSchema.getName())
                .namespace(tableSchema.getNamespace())
                .fields();
        Map<String, String> lowerToGivenName = getCanonicalToGivenFieldName(tableSchema);

        for (String columnName : maskedColumns) {
            Schema.Field field = tableSchema.getField(columnName);
            if (Objects.isNull(field)) {
                if (!lowerToGivenName.containsKey(columnName)) {
                    throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, "Unable to find column %s in table Avro schema %s".formatted(columnName, tableSchema.getFullName()));
                }
                field = tableSchema.getField(lowerToGivenName.get(columnName));
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
                    // To maintain backwards compatibility, invalid defaults are mapped to null.
                    // This behavior defined by io.trino.tests.product.hive.TestAvroSchemaStrictness.testInvalidUnionDefaults.
                    // The solution is to make the field nullable and default-able to null. Any place default would be used, null will be.
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

    private static TrinoInputFile cacheInputIfSmall(Location path, long start, long length, long estimatedFileSize, TrinoFileSystem trinoFileSystem)
    {
        TrinoInputFile inputFile = trinoFileSystem.newInputFile(path);
        try {
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
        catch (RuntimeException | IOException e) {
            throw new TrinoException(HIVE_CANNOT_OPEN_SPLIT, splitError(e, path, start, length), e);
        }
        return inputFile;
    }
}
