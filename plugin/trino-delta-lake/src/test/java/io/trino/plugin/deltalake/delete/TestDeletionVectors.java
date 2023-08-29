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
package io.trino.plugin.deltalake.delete;

import com.google.common.io.Resources;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.deltalake.transactionlog.DeletionVectorEntry;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.OptionalInt;

import static io.trino.plugin.deltalake.DeltaTestingConnectorSession.SESSION;
import static io.trino.plugin.deltalake.delete.DeletionVectors.readDeletionVectors;
import static io.trino.plugin.deltalake.delete.DeletionVectors.toFileName;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDeletionVectors
{
    @Test
    public void testUuidStorageType()
            throws Exception
    {
        // The deletion vector has a deleted row at position 1
        Path path = new File(Resources.getResource("databricks122/deletion_vectors").toURI()).toPath();
        TrinoFileSystem fileSystem = HDFS_FILE_SYSTEM_FACTORY.create(SESSION);
        DeletionVectorEntry deletionVector = new DeletionVectorEntry("u", "R7QFX3rGXPFLhHGq&7g<", OptionalInt.of(1), 34, 1);

        Roaring64NavigableMap bitmaps = readDeletionVectors(fileSystem, Location.of(path.toString()), deletionVector);
        assertThat(bitmaps.getLongCardinality()).isEqualTo(1);
        assertThat(bitmaps.contains(0)).isFalse();
        assertThat(bitmaps.contains(1)).isTrue();
        assertThat(bitmaps.contains(2)).isFalse();
    }

    @Test
    public void testUnsupportedPathStorageType()
    {
        TrinoFileSystem fileSystem = HDFS_FILE_SYSTEM_FACTORY.create(SESSION);
        DeletionVectorEntry deletionVector = new DeletionVectorEntry("p", "s3://bucket/table/deletion_vector.bin", OptionalInt.empty(), 40, 1);
        assertThatThrownBy(() -> readDeletionVectors(fileSystem, Location.of("s3://bucket/table"), deletionVector))
                .hasMessageContaining("Unsupported storage type for deletion vector: p");
    }

    @Test
    public void testUnsupportedInlineStorageType()
    {
        TrinoFileSystem fileSystem = HDFS_FILE_SYSTEM_FACTORY.create(SESSION);
        DeletionVectorEntry deletionVector = new DeletionVectorEntry("i", "wi5b=000010000siXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L", OptionalInt.empty(), 40, 1);
        assertThatThrownBy(() -> readDeletionVectors(fileSystem, Location.of("s3://bucket/table"), deletionVector))
                .hasMessageContaining("Unsupported storage type for deletion vector: i");
    }

    @Test
    public void testToFileName()
    {
        assertThat(toFileName("R7QFX3rGXPFLhHGq&7g<")).isEqualTo("deletion_vector_a52eda8c-0a57-4636-814b-9c165388f7ca.bin");
        assertThat(toFileName("ab^-aqEH.-t@S}K{vb[*k^")).isEqualTo("ab/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin");
    }
}
