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

import com.google.common.collect.ImmutableList;
import io.trino.blob.cache.memory.MemoryBlobCache;
import io.trino.blob.cache.memory.MemoryBlobCacheConfig;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.cache.CacheFileSystem;
import io.trino.filesystem.memory.MemoryFileSystem;
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
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

final class TestCachedManifestStaleLength
{
    private static final Schema SCHEMA = SchemaBuilder.record("Test").namespace("test").fields().requiredString("value").endRecord();

    /**
     * Regression test for <a href="https://github.com/trinodb/trino/issues/25702">#25702</a>.
     * <p>
     * An iceberg manifest list may record a manifest length that is smaller than the actual
     * on-storage size. Uncached, this is harmless: the raw input stream reads to the actual
     * end of the object, so the Avro reader still sees the whole manifest. The blob cache must
     * preserve those semantics — it must not populate the entry with only the declared number
     * of bytes, and it must not serve such a truncated entry to readers that never declared
     * a length.
     */
    @Test
    void testStaleDeclaredLengthReadsFullManifest()
            throws IOException
    {
        MemoryFileSystem delegate = new MemoryFileSystem();
        MemoryBlobCache cache = new MemoryBlobCache(new MemoryBlobCacheConfig());
        TrinoFileSystem fileSystem = new CacheFileSystem(delegate, cache, new IcebergCacheKeyProvider());

        Location location = Location.of("memory:///stale-length-%s-m0.avro".formatted(UUID.randomUUID()));
        AvroFile avroFile = writeTwoBlockAvroFile("first", "second");
        fileSystem.newOutputFile(location).createOrOverwrite(avroFile.content());

        // A stale length that falls exactly on the first Avro block boundary: a truncated
        // read then loses the second record silently instead of failing
        long staleLength = avroFile.firstBlockEnd();
        assertThat(staleLength).isLessThan(avroFile.content().length);

        // Uncached baseline: the raw stream ignores the declared length and reads to the
        // actual end of the file, so both records come back
        assertThat(readValues(delegate.newInputFile(location, staleLength)))
                .containsExactly("first", "second");

        // The same read through the cache must return the same records
        assertThat(readValues(fileSystem.newInputFile(location, staleLength)))
                .containsExactly("first", "second");

        // The populated entry must not poison readers that never declared a length
        assertThat(fileSystem.newInputFile(location).length()).isEqualTo(avroFile.content().length);
        assertThat(readValues(fileSystem.newInputFile(location)))
                .containsExactly("first", "second");
    }

    private static List<String> readValues(TrinoInputFile inputFile)
            throws IOException
    {
        byte[] bytes;
        try (TrinoInputStream stream = inputFile.newStream()) {
            bytes = stream.readAllBytes();
        }
        ImmutableList.Builder<String> values = ImmutableList.builder();
        try (FileReader<GenericRecord> reader = DataFileReader.openReader(new SeekableByteArrayInput(bytes), new GenericDatumReader<GenericRecord>())) {
            while (reader.hasNext()) {
                values.add(reader.next().get("value").toString());
            }
        }
        return values.build();
    }

    private record AvroFile(byte[] content, long firstBlockEnd) {}

    private static AvroFile writeTwoBlockAvroFile(String firstValue, String secondValue)
            throws IOException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        long firstBlockEnd;
        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<GenericRecord>(SCHEMA))) {
            writer.create(SCHEMA, out);
            writer.append(record(firstValue));
            // Force a block boundary, so that the file truncated at this position is still a
            // well-formed Avro file that just misses the second record
            firstBlockEnd = writer.sync();
            writer.append(record(secondValue));
        }
        return new AvroFile(out.toByteArray(), firstBlockEnd);
    }

    private static GenericRecord record(String value)
    {
        GenericRecord record = new GenericData.Record(SCHEMA);
        record.put("value", value);
        return record;
    }
}
