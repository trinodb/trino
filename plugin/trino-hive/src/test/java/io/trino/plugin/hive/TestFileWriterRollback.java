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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.ForwardingTrinoFileSystem;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.spi.Page;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.block.BlockAssertions.createStringsBlock;
import static io.trino.plugin.hive.HiveTestUtils.SESSION;
import static io.trino.plugin.hive.HiveTestUtils.getDefaultHiveFileWriterFactories;
import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMNS;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMN_TYPES;
import static org.assertj.core.api.Assertions.assertThat;

final class TestFileWriterRollback
{
    @ParameterizedTest
    @EnumSource(names = {"ORC", "PARQUET", "AVRO", "RCBINARY", "RCTEXT", "SEQUENCEFILE", "JSON", "OPENX_JSON", "TEXTFILE", "CSV"})
    void testRollbackDoesNotCreateFile(HiveStorageFormat storageFormat)
            throws IOException
    {
        TrinoFileSystem fileSystem = new DeleteIgnoringFileSystem(new MemoryFileSystemFactory().create(SESSION));
        Location location = Location.of("memory:///test/data");
        FileWriter fileWriter = getDefaultHiveFileWriterFactories(new HiveConfig(), _ -> fileSystem).stream()
                .map(fileWriterFactory -> fileWriterFactory.createFileWriter(
                        location,
                        ImmutableList.of("value"),
                        storageFormat.toStorageFormat(),
                        HiveCompressionCodec.NONE,
                        ImmutableMap.of(LIST_COLUMNS, "value", LIST_COLUMN_TYPES, "string"),
                        SESSION,
                        OptionalInt.empty(),
                        NO_ACID_TRANSACTION,
                        false,
                        WriterKind.INSERT))
                .flatMap(Optional::stream)
                .findFirst()
                .orElseThrow();
        fileWriter.appendRows(new Page(createStringsBlock("value")));

        fileWriter.rollback();

        assertThat(fileSystem.newInputFile(location).exists()).isFalse();
    }

    private static final class DeleteIgnoringFileSystem
            extends ForwardingTrinoFileSystem
    {
        private DeleteIgnoringFileSystem(TrinoFileSystem delegate)
        {
            super(delegate);
        }

        @Override
        public void deleteFile(Location location) {}
    }
}
