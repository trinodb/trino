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
import org.apache.avro.InvalidAvroMagicException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestCachedManifestStaleLength
{
    @Test
    public void testStaleDeclaredLengthCausesAvroMagicFailure()
            throws IOException
    {
        // Reproduces https://github.com/trinodb/trino/issues/25702.
        //
        // In production an iceberg manifest list may record a manifest file's
        // length from before the file was rewritten with a larger version,
        // leaving the recorded length smaller than the actual on-storage size.
        // When a reader opens the file with that stale length,
        // MemoryFileSystemCache.load calls readTail(staleLength) and caches only
        // the last staleLength bytes — the cached content begins with bytes from
        // the middle of the file, not the Avro magic header. Avro's
        // DataFileReader then throws InvalidAvroMagicException.

        MemoryFileSystem delegate = new MemoryFileSystem();
        MemoryFileSystemCache cache = new MemoryFileSystemCache(new MemoryFileSystemCacheConfig()
                .setMaxContentLength(DataSize.ofBytes(2L * 1024 * 1024))
                .setCacheTtl(new Duration(8, HOURS)));
        TrinoFileSystem fileSystem = new CacheFileSystem(delegate, cache, new DefaultCacheKeyProvider());

        Location location = Location.of("memory:///stale-length-%s".formatted(UUID.randomUUID()));

        // The file exists on storage in its original (smaller) form.
        byte[] originalContent = "small".getBytes(UTF_8);
        fileSystem.newOutputFile(location).createOrOverwrite(originalContent);

        // The file is rewritten with a larger, valid Avro data file.
        Schema schema = SchemaBuilder.record("Test").namespace("test").fields().requiredString("value").endRecord();
        byte[] avroContent = writeAvroFile(schema, "hello world");
        fileSystem.newOutputFile(location).createOrOverwrite(avroContent);

        // A reader opens the file using the STALE (original, smaller) length.
        TrinoInputFile inputFile = fileSystem.newInputFile(location, originalContent.length);

        // Trigger cache load via readTail(staleLength); the cached entry contains
        // only the last staleLength bytes of the Avro file, missing the magic
        // header at the beginning.
        try (TrinoInput input = inputFile.newInput()) {
            input.readTail(originalContent.length);
        }

        // Read the cached content and feed it to Avro's DataFileReader, mirroring
        // what iceberg does when reading a manifest. The magic check fails because
        // the cache returned the tail of the file, not the start.
        byte[] cachedBytes;
        try (TrinoInputStream stream = inputFile.newStream()) {
            cachedBytes = stream.readAllBytes();
        }

        assertThatThrownBy(() -> DataFileReader.openReader(
                new SeekableByteArrayInput(cachedBytes),
                new GenericDatumReader<GenericRecord>()))
                .isInstanceOf(InvalidAvroMagicException.class)
                .hasMessage("Not an Avro data file");
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
