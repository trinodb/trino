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
package io.trino.plugin.iceberg.cache;

import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.cache.CacheFileSystem;
import io.trino.filesystem.cache.DefaultCacheKeyProvider;
import io.trino.filesystem.memory.MemoryFileSystem;
import io.trino.filesystem.memory.MemoryFileSystemCache;
import io.trino.filesystem.memory.MemoryFileSystemCacheConfig;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCachedManifestStaleLength
{
    @Test
    public void testStaleDeclaredLengthPreservesAvroFileHeader()
            throws IOException
    {
        // Regression test for https://github.com/trinodb/trino/issues/25702.
        //
        // In production an iceberg manifest list may record a manifest file's
        // length from before the file was rewritten with a larger version,
        // leaving the recorded length smaller than the actual on-storage size.
        // When a reader opens the file with that stale length the cache must
        // still load the full file from position 0, otherwise the Avro magic
        // header would be missing from the cached content and the reader would
        // throw InvalidAvroMagicException.

        MemoryFileSystem delegate = new MemoryFileSystem();
        MemoryFileSystemCache cache = new MemoryFileSystemCache(new MemoryFileSystemCacheConfig()
                .setMaxContentLength(DataSize.ofBytes(2L * 1024 * 1024))
                .setCacheTtl(new Duration(8, HOURS)));
        TrinoFileSystem fileSystem = new CacheFileSystem(delegate, cache, new DefaultCacheKeyProvider());

        Location location = Location.of("memory:///stale-length-%s".formatted(UUID.randomUUID()));

        byte[] originalContent = "small".getBytes(UTF_8);
        fileSystem.newOutputFile(location).createOrOverwrite(originalContent);

        Schema schema = SchemaBuilder.record("Test").namespace("test").fields().requiredString("value").endRecord();
        byte[] avroContent = writeAvroFile(schema, "hello world");
        fileSystem.newOutputFile(location).createOrOverwrite(avroContent);

        TrinoInputFile inputFile = fileSystem.newInputFile(location, originalContent.length);

        // Trigger cache load via the stale (smaller) length. The cache must load
        // the full file from position 0 rather than only the tail.
        try (TrinoInput input = inputFile.newInput()) {
            input.readTail(originalContent.length);
        }

        byte[] cachedBytes;
        try (TrinoInputStream stream = inputFile.newStream()) {
            cachedBytes = stream.readAllBytes();
        }

        try (FileReader<GenericRecord> reader = DataFileReader.openReader(
                new SeekableByteArrayInput(cachedBytes),
                new GenericDatumReader<>())) {
            assertThat(reader.hasNext()).isTrue();
            assertThat(reader.next().get("value")).hasToString("hello world");
        }
    }

    private static byte[] writeAvroFile(Schema schema, String value)
            throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
            writer.create(schema, baos);
            GenericRecord record = new GenericData.Record(schema);
            record.put("value", value);
            writer.append(record);
        }
        return baos.toByteArray();
    }
}
