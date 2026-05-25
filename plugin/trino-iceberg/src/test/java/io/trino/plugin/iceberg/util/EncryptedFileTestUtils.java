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

import com.google.common.collect.ImmutableList;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.apache.iceberg.TableProperties.ENCRYPTION_TABLE_KEY;

public final class EncryptedFileTestUtils
{
    private EncryptedFileTestUtils() {}

    /**
     * Creates an EncryptionManager for a table using the given KeyManagementClient.
     * The table properties must contain ENCRYPTION_TABLE_KEY.
     */
    public static EncryptionManager createEncryptionManager(Map<String, String> tableProperties, KeyManagementClient kmsClient)
    {
        return EncryptionUtil.createEncryptionManager(ImmutableList.of(), tableProperties, kmsClient);
    }

    /**
     * Creates an EncryptionManager using explicit properties (not from table).
     * Use when encryption key is set out-of-band (e.g. in metastore params) rather than as table property.
     */
    public static EncryptionManager createEncryptionManager(String keyId, KeyManagementClient kmsClient)
    {
        return EncryptionUtil.createEncryptionManager(
                ImmutableList.of(),
                Map.of(ENCRYPTION_TABLE_KEY, keyId),
                kmsClient);
    }

    public static DataFile writeEncryptedDataFile(Table table, EncryptionManager encryptionManager, List<Record> records)
            throws Exception
    {
        String dataPath = Path.of(table.location())
                .resolve("data")
                .resolve("enc-data-" + randomNameSuffix() + ".parquet")
                .toString();

        EncryptedOutputFile encryptedOutputFile = encryptionManager.encrypt(table.io().newOutputFile(dataPath));
        try (DataWriter<Record> writer = Parquet.writeData(encryptedOutputFile)
                .forTable(table)
                .withSpec(table.spec())
                .withPartition(null)
                .withKeyMetadata(encryptedOutputFile.keyMetadata())
                .createWriterFunc(GenericParquetWriter::create)
                .build()) {
            for (Record record : records) {
                writer.write(record);
            }
            writer.close();
            return writer.toDataFile();
        }
    }

    public static DataFile writeEncryptedAvroDataFile(Table table, EncryptionManager encryptionManager, List<Record> records)
            throws Exception
    {
        String dataPath = Path.of(table.location())
                .resolve("data")
                .resolve("enc-data-" + randomNameSuffix() + ".avro")
                .toString();

        EncryptedOutputFile encryptedOutputFile = encryptionManager.encrypt(table.io().newOutputFile(dataPath));
        try (org.apache.iceberg.io.DataWriter<Record> writer = Avro.writeData(encryptedOutputFile)
                .forTable(table)
                .withSpec(table.spec())
                .withPartition(null)
                .withKeyMetadata(encryptedOutputFile.keyMetadata())
                .createWriterFunc(org.apache.iceberg.data.avro.DataWriter::create)
                .build()) {
            for (Record record : records) {
                writer.write(record);
            }
            writer.close();
            return writer.toDataFile();
        }
    }

    public static DeleteFile writeEncryptedPositionDeleteFile(
            Table table,
            EncryptionManager encryptionManager,
            String dataFilePath,
            long... positions)
            throws Exception
    {
        String deletePath = Path.of(table.location())
                .resolve("data")
                .resolve("enc-pos-delete-" + randomNameSuffix() + ".parquet")
                .toString();

        EncryptedOutputFile encryptedOutputFile = encryptionManager.encrypt(table.io().newOutputFile(deletePath));
        try (PositionDeleteWriter<Record> writer = Parquet.writeDeletes(encryptedOutputFile)
                .withSpec(PartitionSpec.unpartitioned())
                .overwrite()
                .withKeyMetadata(encryptedOutputFile.keyMetadata())
                .buildPositionWriter()) {
            PositionDelete<Record> positionDelete = PositionDelete.create();
            for (long position : positions) {
                writer.write(positionDelete.set(dataFilePath, position, null));
            }
            writer.close();
            return writer.toDeleteFile();
        }
    }

    public static DeleteFile writeEncryptedEqualityDeleteFile(
            Table table,
            EncryptionManager encryptionManager,
            Schema deleteRowSchema,
            List<Integer> equalityFieldIds,
            List<Record> deleteRecords)
            throws Exception
    {
        String deletePath = Path.of(table.location())
                .resolve("data")
                .resolve("enc-eq-delete-" + randomNameSuffix() + ".parquet")
                .toString();

        EncryptedOutputFile encryptedOutputFile = encryptionManager.encrypt(table.io().newOutputFile(deletePath));
        EqualityDeleteWriter<Record> writer = Parquet.writeDeletes(encryptedOutputFile)
                .forTable(table)
                .rowSchema(deleteRowSchema)
                .withSpec(PartitionSpec.unpartitioned())
                .equalityFieldIds(equalityFieldIds)
                .overwrite()
                .withKeyMetadata(encryptedOutputFile.keyMetadata())
                .createWriterFunc(GenericParquetWriter::create)
                .buildEqualityWriter();

        try (Closeable ignored = writer) {
            for (Record record : deleteRecords) {
                writer.write(record);
            }
        }

        return writer.toDeleteFile();
    }

    /**
     * Writes a plaintext (unencrypted) Parquet data file.
     */
    public static DataFile writePlaintextDataFile(Table table, List<Record> records)
            throws Exception
    {
        String dataPath = Path.of(table.location())
                .resolve("data")
                .resolve("plain-" + randomNameSuffix() + ".parquet")
                .toString();

        try (DataWriter<Record> writer = Parquet.writeData(table.io().newOutputFile(dataPath))
                .forTable(table)
                .withSpec(table.spec())
                .withPartition(null)
                .createWriterFunc(GenericParquetWriter::create)
                .build()) {
            for (Record record : records) {
                writer.write(record);
            }
            writer.close();
            return writer.toDataFile();
        }
    }

    /**
     * Writes a plaintext Parquet file but attaches valid encryption key metadata,
     * simulating a corrupted or tampered file.
     */
    public static DataFile writePlaintextFileWithEncryptionKeyMetadata(
            Table table,
            EncryptionManager encryptionManager,
            List<Record> records)
            throws Exception
    {
        String dataPath = Path.of(table.location())
                .resolve("data")
                .resolve("fake-enc-" + randomNameSuffix() + ".parquet")
                .toString();

        EncryptedOutputFile encryptedOutputFile = encryptionManager.encrypt(table.io().newOutputFile(dataPath));

        try (DataWriter<Record> writer = Parquet.writeData(table.io().newOutputFile(dataPath))
                .forTable(table)
                .withSpec(table.spec())
                .withPartition(null)
                .createWriterFunc(GenericParquetWriter::create)
                .build()) {
            for (Record record : records) {
                writer.write(record);
            }
            writer.close();
            DataFile plaintextFile = writer.toDataFile();

            return DataFiles.builder(table.spec())
                    .withPath(plaintextFile.path().toString())
                    .withFileSizeInBytes(plaintextFile.fileSizeInBytes())
                    .withRecordCount(plaintextFile.recordCount())
                    .withFormat(plaintextFile.format())
                    .withMetrics(new Metrics(
                            plaintextFile.recordCount(),
                            plaintextFile.columnSizes(),
                            plaintextFile.valueCounts(),
                            plaintextFile.nullValueCounts(),
                            plaintextFile.nanValueCounts(),
                            plaintextFile.lowerBounds(),
                            plaintextFile.upperBounds()))
                    .withEncryptionKeyMetadata(encryptedOutputFile.keyMetadata())
                    .build();
        }
    }

    public static List<Record> createTestRecords(Schema schema, int count)
    {
        return IntStream.range(0, count)
                .mapToObj(i -> {
                    Record record = GenericRecord.create(schema);
                    record.setField("id", i);
                    record.setField("age", count - 1 - i);
                    return record;
                })
                .toList();
    }

    public static List<Record> createTestRecordsWithName(Schema schema, int count)
    {
        return IntStream.range(0, count)
                .mapToObj(i -> {
                    Record record = GenericRecord.create(schema);
                    record.setField("id", i);
                    record.setField("name", "name_" + i);
                    return record;
                })
                .toList();
    }
}
