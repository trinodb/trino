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
package io.trino.orc;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.orc.metadata.ColumnMetadata;
import io.trino.orc.metadata.CompressionKind;
import io.trino.orc.metadata.ExceptionWrappingMetadataReader;
import io.trino.orc.metadata.Footer;
import io.trino.orc.metadata.Metadata;
import io.trino.orc.metadata.OrcColumnId;
import io.trino.orc.metadata.OrcMetadataReader;
import io.trino.orc.metadata.OrcType;
import io.trino.orc.metadata.OrcType.OrcTypeKind;
import io.trino.orc.metadata.PostScript;
import io.trino.orc.metadata.PostScript.HiveWriterVersion;
import io.trino.orc.stream.OrcChunkLoader;
import io.trino.orc.stream.OrcInputStream;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.orc.OrcDecompressor.createOrcDecompressor;
import static io.trino.orc.metadata.OrcColumnId.ROOT_COLUMN;
import static io.trino.orc.metadata.PostScript.MAGIC;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static org.joda.time.DateTimeZone.UTC;

public class OrcReader
{
    public static final int MAX_BATCH_SIZE = 8196;
    public static final int INITIAL_BATCH_SIZE = 1;
    public static final int BATCH_SIZE_GROWTH_FACTOR = 2;

    private static final Logger log = Logger.get(OrcReader.class);

    private static final int CURRENT_MAJOR_VERSION = 0;
    private static final int CURRENT_MINOR_VERSION = 12;
    private static final int EXPECTED_FOOTER_SIZE = 16 * 1024;

    private final OrcDataSource orcDataSource;
    private final ExceptionWrappingMetadataReader metadataReader;
    private final OrcReaderOptions options;
    private final HiveWriterVersion hiveWriterVersion;
    private final int bufferSize;
    private final CompressionKind compressionKind;
    private final Optional<OrcDecompressor> decompressor;
    private final Footer footer;
    private final Metadata metadata;
    private final OrcColumn rootColumn;

    private final Optional<OrcWriteValidation> writeValidation;

    public static Optional<OrcReader> createOrcReader(OrcDataSource orcDataSource, OrcReaderOptions options)
            throws IOException
    {
        return createOrcReader(orcDataSource, options, Optional.empty());
    }

    private static Optional<OrcReader> createOrcReader(
            OrcDataSource orcDataSource,
            OrcReaderOptions options,
            Optional<OrcWriteValidation> writeValidation)
            throws IOException
    {
        orcDataSource = wrapWithCacheIfTiny(orcDataSource, options.getTinyStripeThreshold());

        // read the tail of the file, and check if the file is actually empty
        long estimatedFileSize = orcDataSource.getEstimatedSize();
        if (estimatedFileSize > 0 && estimatedFileSize <= MAGIC.length()) {
            throw new OrcCorruptionException(orcDataSource.getId(), "Invalid file size %s", estimatedFileSize);
        }

        long expectedReadSize = min(estimatedFileSize, EXPECTED_FOOTER_SIZE);
        Slice fileTail = orcDataSource.readTail(toIntExact(expectedReadSize));
        if (fileTail.length() == 0) {
            return Optional.empty();
        }

        return Optional.of(new OrcReader(orcDataSource, options, writeValidation, fileTail));
    }

    private OrcReader(
            OrcDataSource orcDataSource,
            OrcReaderOptions options,
            Optional<OrcWriteValidation> writeValidation,
            Slice fileTail)
            throws IOException
    {
        this.options = requireNonNull(options, "options is null");
        this.orcDataSource = orcDataSource;
        this.metadataReader = new ExceptionWrappingMetadataReader(orcDataSource.getId(), new OrcMetadataReader());

        this.writeValidation = requireNonNull(writeValidation, "writeValidation is null");

        //
        // Read the file tail:
        //
        // variable: Footer
        // variable: Metadata
        // variable: PostScript - contains length of footer and metadata
        // 1 byte: postScriptSize

        // get length of PostScript - last byte of the file
        int postScriptSize = fileTail.getUnsignedByte(fileTail.length() - SIZE_OF_BYTE);
        if (postScriptSize >= fileTail.length()) {
            throw new OrcCorruptionException(orcDataSource.getId(), "Invalid postscript length %s", postScriptSize);
        }

        // decode the post script
        PostScript postScript;
        try {
            postScript = metadataReader.readPostScript(fileTail.slice(fileTail.length() - SIZE_OF_BYTE - postScriptSize, postScriptSize).getInput());
        }
        catch (OrcCorruptionException e) {
            // check if this is an ORC file and not an RCFile or something else
            try {
                Slice headerMagic = orcDataSource.readFully(0, MAGIC.length());
                if (!MAGIC.equals(headerMagic)) {
                    throw new OrcCorruptionException(orcDataSource.getId(), "Not an ORC file");
                }
            }
            catch (IOException ignored) {
                // throw original exception
            }

            throw e;
        }

        // verify this is a supported version
        checkOrcVersion(orcDataSource, postScript.getVersion());
        validateWrite(validation -> validation.getVersion().equals(postScript.getVersion()), "Unexpected version");

        this.bufferSize = toIntExact(postScript.getCompressionBlockSize());

        // check compression codec is supported
        this.compressionKind = postScript.getCompression();
        this.decompressor = createOrcDecompressor(orcDataSource.getId(), compressionKind, bufferSize);
        validateWrite(validation -> validation.getCompression() == compressionKind, "Unexpected compression");

        this.hiveWriterVersion = postScript.getHiveWriterVersion();

        int footerSize = toIntExact(postScript.getFooterLength());
        int metadataSize = toIntExact(postScript.getMetadataLength());

        // check if extra bytes need to be read
        Slice completeFooterSlice;
        int completeFooterSize = footerSize + metadataSize + postScriptSize + SIZE_OF_BYTE;
        if (completeFooterSize > fileTail.length()) {
            // initial read was not large enough, so just read again with the correct size
            completeFooterSlice = orcDataSource.readTail(completeFooterSize);
        }
        else {
            // footer is already in the bytes in fileTail, just adjust position, length
            completeFooterSlice = fileTail.slice(fileTail.length() - completeFooterSize, completeFooterSize);
        }

        // read metadata
        Slice metadataSlice = completeFooterSlice.slice(0, metadataSize);
        try (InputStream metadataInputStream = new OrcInputStream(OrcChunkLoader.create(orcDataSource.getId(), metadataSlice, decompressor, newSimpleAggregatedMemoryContext()))) {
            this.metadata = metadataReader.readMetadata(hiveWriterVersion, metadataInputStream);
        }

        // read footer
        Slice footerSlice = completeFooterSlice.slice(metadataSize, footerSize);
        try (InputStream footerInputStream = new OrcInputStream(OrcChunkLoader.create(orcDataSource.getId(), footerSlice, decompressor, newSimpleAggregatedMemoryContext()))) {
            this.footer = metadataReader.readFooter(hiveWriterVersion, footerInputStream);
        }
        if (footer.getTypes().size() == 0) {
            throw new OrcCorruptionException(orcDataSource.getId(), "File has no columns");
        }

        this.rootColumn = createOrcColumn("", "", new OrcColumnId(0), footer.getTypes(), orcDataSource.getId());

        validateWrite(validation -> validation.getColumnNames().equals(getColumnNames()), "Unexpected column names");
        validateWrite(validation -> validation.getRowGroupMaxRowCount() == footer.getRowsInRowGroup().orElse(0), "Unexpected rows in group");
        if (writeValidation.isPresent()) {
            writeValidation.get().validateMetadata(orcDataSource.getId(), footer.getUserMetadata());
            writeValidation.get().validateFileStatistics(orcDataSource.getId(), footer.getFileStats());
            writeValidation.get().validateStripeStatistics(orcDataSource.getId(), footer.getStripes(), metadata.getStripeStatsList());
        }
    }

    public List<String> getColumnNames()
    {
        return footer.getTypes().get(ROOT_COLUMN).getFieldNames();
    }

    public Footer getFooter()
    {
        return footer;
    }

    public Metadata getMetadata()
    {
        return metadata;
    }

    public OrcColumn getRootColumn()
    {
        return rootColumn;
    }

    public int getBufferSize()
    {
        return bufferSize;
    }

    public CompressionKind getCompressionKind()
    {
        return compressionKind;
    }

    public OrcRecordReader createRecordReader(
            List<OrcColumn> readColumns,
            List<Type> readTypes,
            OrcPredicate predicate,
            DateTimeZone legacyFileTimeZone,
            AggregatedMemoryContext memoryUsage,
            int initialBatchSize,
            Function<Exception, RuntimeException> exceptionTransform)
            throws OrcCorruptionException
    {
        return createRecordReader(
                readColumns,
                readTypes,
                Collections.nCopies(readColumns.size(), fullyProjectedLayout()),
                predicate,
                0,
                orcDataSource.getEstimatedSize(),
                legacyFileTimeZone,
                memoryUsage,
                initialBatchSize,
                exceptionTransform,
                NameBasedFieldMapper::create);
    }

    public OrcRecordReader createRecordReader(
            List<OrcColumn> readColumns,
            List<Type> readTypes,
            List<ProjectedLayout> readLayouts,
            OrcPredicate predicate,
            long offset,
            long length,
            DateTimeZone legacyFileTimeZone,
            AggregatedMemoryContext memoryUsage,
            int initialBatchSize,
            Function<Exception, RuntimeException> exceptionTransform,
            FieldMapperFactory fieldMapperFactory)
            throws OrcCorruptionException
    {
        return new OrcRecordReader(
                requireNonNull(readColumns, "readColumns is null"),
                requireNonNull(readTypes, "readTypes is null"),
                requireNonNull(readLayouts, "readLayouts is null"),
                requireNonNull(predicate, "predicate is null"),
                footer.getNumberOfRows(),
                footer.getStripes(),
                footer.getFileStats(),
                metadata.getStripeStatsList(),
                orcDataSource,
                offset,
                length,
                footer.getTypes(),
                decompressor,
                footer.getRowsInRowGroup(),
                requireNonNull(legacyFileTimeZone, "legacyFileTimeZone is null"),
                hiveWriterVersion,
                metadataReader,
                options,
                footer.getUserMetadata(),
                memoryUsage,
                writeValidation,
                initialBatchSize,
                exceptionTransform,
                fieldMapperFactory);
    }

    private static OrcDataSource wrapWithCacheIfTiny(OrcDataSource dataSource, DataSize maxCacheSize)
            throws IOException
    {
        if (dataSource instanceof MemoryOrcDataSource || dataSource instanceof CachingOrcDataSource) {
            return dataSource;
        }
        if (dataSource.getEstimatedSize() > maxCacheSize.toBytes()) {
            return dataSource;
        }
        Slice data = dataSource.readTail(toIntExact(dataSource.getEstimatedSize()));
        dataSource.close();
        return new MemoryOrcDataSource(dataSource.getId(), data);
    }

    private static OrcColumn createOrcColumn(
            String parentStreamName,
            String fieldName,
            OrcColumnId columnId,
            ColumnMetadata<OrcType> types,
            OrcDataSourceId orcDataSourceId)
    {
        String path = fieldName.isEmpty() ? parentStreamName : parentStreamName + "." + fieldName;
        OrcType orcType = types.get(columnId);

        List<OrcColumn> nestedColumns = ImmutableList.of();
        if (orcType.getOrcTypeKind() == OrcTypeKind.STRUCT) {
            nestedColumns = IntStream.range(0, orcType.getFieldCount())
                    .mapToObj(fieldId -> createOrcColumn(
                            path,
                            orcType.getFieldName(fieldId),
                            orcType.getFieldTypeIndex(fieldId),
                            types,
                            orcDataSourceId))
                    .collect(toImmutableList());
        }
        else if (orcType.getOrcTypeKind() == OrcTypeKind.LIST) {
            nestedColumns = ImmutableList.of(createOrcColumn(path, "item", orcType.getFieldTypeIndex(0), types, orcDataSourceId));
        }
        else if (orcType.getOrcTypeKind() == OrcTypeKind.MAP) {
            nestedColumns = ImmutableList.of(
                    createOrcColumn(path, "key", orcType.getFieldTypeIndex(0), types, orcDataSourceId),
                    createOrcColumn(path, "value", orcType.getFieldTypeIndex(1), types, orcDataSourceId));
        }
        else if (orcType.getOrcTypeKind() == OrcTypeKind.UNION) {
            nestedColumns = IntStream.range(0, orcType.getFieldCount())
                    .mapToObj(fieldId -> createOrcColumn(
                            path,
                            "field" + fieldId,
                            orcType.getFieldTypeIndex(fieldId),
                            types,
                            orcDataSourceId))
                    .collect(toImmutableList());
        }
        return new OrcColumn(path, columnId, fieldName, orcType.getOrcTypeKind(), orcDataSourceId, nestedColumns, orcType.getAttributes());
    }

    /**
     * Check to see if this ORC file is from a future version and if so,
     * warn the user that we may not be able to read all of the column encodings.
     */
    // This is based on the Apache Hive ORC code
    private static void checkOrcVersion(OrcDataSource orcDataSource, List<Integer> version)
    {
        if (version.size() >= 1) {
            int major = version.get(0);
            int minor = 0;
            if (version.size() > 1) {
                minor = version.get(1);
            }

            if (major > CURRENT_MAJOR_VERSION || (major == CURRENT_MAJOR_VERSION && minor > CURRENT_MINOR_VERSION)) {
                log.warn("ORC file %s was written by a newer Hive version %s. This file may not be readable by this version of Hive (%s.%s).",
                        orcDataSource,
                        Joiner.on('.').join(version),
                        CURRENT_MAJOR_VERSION,
                        CURRENT_MINOR_VERSION);
            }
        }
    }

    private void validateWrite(Predicate<OrcWriteValidation> test, String messageFormat, Object... args)
            throws OrcCorruptionException
    {
        if (writeValidation.isPresent() && !test.test(writeValidation.get())) {
            throw new OrcCorruptionException(orcDataSource.getId(), "Write validation failed: " + messageFormat, args);
        }
    }

    static void validateFile(
            OrcWriteValidation writeValidation,
            OrcDataSource input,
            List<Type> readTypes)
            throws OrcCorruptionException
    {
        try {
            OrcReader orcReader = createOrcReader(input, new OrcReaderOptions(), Optional.of(writeValidation))
                    .orElseThrow(() -> new OrcCorruptionException(input.getId(), "File is empty"));
            try (OrcRecordReader orcRecordReader = orcReader.createRecordReader(
                    orcReader.getRootColumn().getNestedColumns(),
                    readTypes,
                    OrcPredicate.TRUE,
                    UTC,
                    newSimpleAggregatedMemoryContext(),
                    INITIAL_BATCH_SIZE,
                    exception -> {
                        throwIfUnchecked(exception);
                        return new RuntimeException(exception);
                    })) {
                for (Page page = orcRecordReader.nextPage(); page != null; page = orcRecordReader.nextPage()) {
                    // fully load the page
                    page.getLoadedPage();
                }
            }
        }
        catch (IOException e) {
            throw new OrcCorruptionException(e, input.getId(), "Validation failed");
        }
    }

    public interface ProjectedLayout
    {
        ProjectedLayout getFieldLayout(OrcColumn orcColumn);
    }

    /**
     * Constructs a ProjectedLayout where all subfields must be read
     */
    public static ProjectedLayout fullyProjectedLayout()
    {
        return orcColumn -> fullyProjectedLayout();
    }

    public static class NameBasedProjectedLayout
            implements ProjectedLayout
    {
        private final Optional<Map<String, ProjectedLayout>> fieldLayouts;

        private NameBasedProjectedLayout(Optional<Map<String, ProjectedLayout>> fieldLayouts)
        {
            this.fieldLayouts = requireNonNull(fieldLayouts, "fieldLayouts is null");
        }

        @Override
        public ProjectedLayout getFieldLayout(OrcColumn orcColumn)
        {
            String name = orcColumn.getColumnName().toLowerCase(ENGLISH);
            if (fieldLayouts.isPresent()) {
                return fieldLayouts.get().get(name);
            }

            return fullyProjectedLayout();
        }

        public static ProjectedLayout createProjectedLayout(OrcColumn root, List<List<String>> dereferences)
        {
            if (dereferences.stream().map(List::size).anyMatch(Predicate.isEqual(0))) {
                return fullyProjectedLayout();
            }

            Map<String, List<List<String>>> dereferencesByField = dereferences.stream().collect(
                    Collectors.groupingBy(
                            sequence -> sequence.get(0),
                            mapping(sequence -> sequence.subList(1, sequence.size()), toList())));

            ImmutableMap.Builder<String, ProjectedLayout> fieldLayouts = ImmutableMap.builder();
            for (OrcColumn nestedColumn : root.getNestedColumns()) {
                String fieldName = nestedColumn.getColumnName().toLowerCase(ENGLISH);
                if (dereferencesByField.containsKey(fieldName)) {
                    fieldLayouts.put(fieldName, createProjectedLayout(nestedColumn, dereferencesByField.get(fieldName)));
                }
            }

            return new NameBasedProjectedLayout(Optional.of(fieldLayouts.buildOrThrow()));
        }
    }

    public interface FieldMapperFactory
    {
        FieldMapper create(OrcColumn orcColumn);
    }

    // Used for mapping a nested field with the appropriate OrcColumn
    public interface FieldMapper
    {
        OrcColumn get(String fieldName);
    }
}
