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
package io.trino.plugin.iceberg.functions.tablefiles;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.iceberg.fileio.ForwardingFileIo;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.function.table.TableFunctionProcessorState;
import io.trino.spi.function.table.TableFunctionSplitProcessor;
import io.trino.spi.type.Type;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.trino.plugin.base.util.JsonUtils.jsonFactory;
import static io.trino.plugin.iceberg.functions.tablefiles.TableFilesFunction.CONTENT_TYPE;
import static io.trino.plugin.iceberg.functions.tablefiles.TableFilesFunction.FILE_FORMAT_TYPE;
import static io.trino.plugin.iceberg.functions.tablefiles.TableFilesFunction.FILE_PATH_TYPE;
import static io.trino.plugin.iceberg.functions.tablefiles.TableFilesFunction.FILE_SIZE_IN_BYTES_TYPE;
import static io.trino.plugin.iceberg.functions.tablefiles.TableFilesFunction.PARTITION_TYPE;
import static io.trino.plugin.iceberg.functions.tablefiles.TableFilesFunction.RECORD_COUNT_TYPE;
import static io.trino.plugin.iceberg.functions.tablefiles.TableFilesFunction.SPEC_ID_TYPE;
import static io.trino.spi.function.table.TableFunctionProcessorState.Finished.FINISHED;
import static io.trino.spi.function.table.TableFunctionProcessorState.Processed.produced;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class TableFilesFunctionProcessor
        implements TableFunctionSplitProcessor
{
    private static final JsonFactory FACTORY = jsonFactory();
    private static final List<Type> PAGE_BUILDER_TYPES = ImmutableList.of(
            CONTENT_TYPE, FILE_PATH_TYPE, FILE_FORMAT_TYPE, SPEC_ID_TYPE, PARTITION_TYPE, RECORD_COUNT_TYPE, FILE_SIZE_IN_BYTES_TYPE);

    private final Closer closer;
    private final Iterator<? extends ContentFile<?>> contentItr;
    private final PageBuilder pageBuilder;

    public TableFilesFunctionProcessor(TrinoFileSystem trinoFileSystem, TableFilesSplit split)
    {
        requireNonNull(split, "split is null");
        this.closer = Closer.create();
        try {
            ManifestFile manifestFile = ManifestFiles.decode(Base64.getDecoder().decode(split.manifestFileEncoded()));
            ManifestReader<? extends ContentFile<?>> manifestReader = closer.register(manifestFile.content() == ManifestContent.DATA ?
                    ManifestFiles.read(manifestFile, new ForwardingFileIo(trinoFileSystem)) :
                    ManifestFiles.readDeleteManifest(manifestFile, new ForwardingFileIo(trinoFileSystem),
                            split.partitionSpecsByIdJson().entrySet().stream().collect(Collectors.toMap(
                                    Map.Entry::getKey,
                                    kv -> PartitionSpecParser.fromJson(SchemaParser.fromJson(split.schemaJson()), kv.getValue())))));
            this.contentItr = manifestReader.iterator();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        // content, file_path, file_format, spec_id, partition, record_count, file_size_in_bytes
        this.pageBuilder = new PageBuilder(PAGE_BUILDER_TYPES);
    }

    @Override
    public TableFunctionProcessorState process()
    {
        try {
            while (contentItr.hasNext() && !pageBuilder.isFull()) {
                pageBuilder.declarePosition();
                ContentFile<?> dataFileMeta = contentItr.next();
                // content
                INTEGER.writeLong(pageBuilder.getBlockBuilder(0), dataFileMeta.content().id());
                // file_path
                VARCHAR.writeString(pageBuilder.getBlockBuilder(1), dataFileMeta.location());
                // file_format
                VARCHAR.writeString(pageBuilder.getBlockBuilder(2), dataFileMeta.format().toString());
                // spec_id
                INTEGER.writeLong(pageBuilder.getBlockBuilder(3), dataFileMeta.specId());

                // partitions
                String partitionDataJson = partitionDataToJson((PartitionData) dataFileMeta.partition());

                if (partitionDataJson == null || partitionDataJson.isEmpty()) {
                    pageBuilder.getBlockBuilder(4).appendNull();
                }
                else {
                    VARCHAR.writeString(pageBuilder.getBlockBuilder(4), partitionDataJson);
                }

                // record_count
                BIGINT.writeLong(pageBuilder.getBlockBuilder(5), dataFileMeta.recordCount());
                // file_size_in_bytes
                BIGINT.writeLong(pageBuilder.getBlockBuilder(6), dataFileMeta.fileSizeInBytes());
            }

            if (!pageBuilder.isEmpty()) {
                Page page = pageBuilder.build();
                pageBuilder.reset();
                return produced(page);
            }
        }
        catch (Exception e) {
            close();
        }

        close();
        return FINISHED;
    }

    public void close()
    {
        try {
            closer.close();
        }
        catch (IOException ex) {
            // ignored
        }
    }

    public static String partitionDataToJson(PartitionData partitionData)
    {
        try {
            StringWriter writer = new StringWriter();
            JsonGenerator generator = FACTORY.createGenerator(writer);
            generator.writeStartObject();
            for (int i = 0; i < partitionData.size(); i++) {
                generator.writeObjectField(partitionData.getPartitionType().fields().get(i).name(), partitionData.get(i));
            }
            generator.writeEndObject();
            generator.flush();
            return writer.toString();
        }
        catch (IOException e) {
            throw new UncheckedIOException("JSON conversion failed for: " + partitionData, e);
        }
    }
}
