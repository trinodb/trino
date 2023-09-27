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
package io.trino.plugin.iceberg.util;

import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.iceberg.PartitionData;
import io.trino.plugin.iceberg.fileio.ForwardingFileIo;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.parquet.Parquet;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.testing.TestingConnectorSession.SESSION;

public final class EqualityDeleteUtils
{
    private EqualityDeleteUtils() {}

    public static void writeEqualityDeleteForTable(
            Table icebergTable,
            TrinoFileSystemFactory fileSystemFactory,
            Optional<PartitionSpec> partitionSpec,
            Optional<PartitionData> partitionData,
            Map<String, Object> overwriteValues,
            Optional<List<String>> deleteFileColumns)
            throws IOException
    {
        List<String> deleteColumns = deleteFileColumns.orElse(new ArrayList<>(overwriteValues.keySet()));
        Schema deleteRowSchema = icebergTable.schema().select(deleteColumns);
        List<Integer> equalityDeleteFieldIds = deleteColumns.stream()
                .map(name -> deleteRowSchema.findField(name).fieldId())
                .collect(toImmutableList());
        writeEqualityDeleteForTableWithSchema(icebergTable, fileSystemFactory, partitionSpec, partitionData, deleteRowSchema, equalityDeleteFieldIds, overwriteValues);
    }

    public static void writeEqualityDeleteForTableWithSchema(
            Table icebergTable,
            TrinoFileSystemFactory fileSystemFactory,
            Optional<PartitionSpec> partitionSpec,
            Optional<PartitionData> partitionData,
            Schema deleteRowSchema,
            List<Integer> equalityDeleteFieldIds,
            Map<String, Object> overwriteValues)
            throws IOException
    {
        String deleteFileName = "local:///delete_file_" + UUID.randomUUID() + ".parquet";
        FileIO fileIo = new ForwardingFileIo(fileSystemFactory.create(SESSION));

        Parquet.DeleteWriteBuilder writerBuilder = Parquet.writeDeletes(fileIo.newOutputFile(deleteFileName))
                .forTable(icebergTable)
                .rowSchema(deleteRowSchema)
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .equalityFieldIds(equalityDeleteFieldIds)
                .overwrite();
        if (partitionSpec.isPresent() && partitionData.isPresent()) {
            writerBuilder = writerBuilder
                    .withSpec(partitionSpec.get())
                    .withPartition(partitionData.get());
        }
        EqualityDeleteWriter<Record> writer = writerBuilder.buildEqualityWriter();

        Record dataDelete = GenericRecord.create(deleteRowSchema);
        try (Closeable ignored = writer) {
            writer.write(dataDelete.copy(overwriteValues));
        }

        icebergTable.newRowDelta().addDeletes(writer.toDeleteFile()).commit();
    }
}
