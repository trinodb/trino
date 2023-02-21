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
package io.trino.plugin.deltalake.transactionlog;

import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.plugin.deltalake.DeltaTestingConnectorSession;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.stream;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static java.util.stream.LongStream.rangeClosed;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestDeltaLakeTransactionLogIterator
{
    @Test
    public void testIterator()
            throws Exception
    {
        TrinoFileSystem fileSystem = new HdfsFileSystemFactory(HDFS_ENVIRONMENT).create(DeltaTestingConnectorSession.SESSION);
        URI resource = getClass().getClassLoader().getResource("deltalake/person").toURI();
        Path tablePath = new Path(resource);

        assertEquals(
                stream(new DeltaLakeTransactionLogIterator(fileSystem, tablePath, Optional.empty(), Optional.empty()))
                        .collect(toImmutableList()).size(),
                14);
        assertThat(getCommitVersions(fileSystem, tablePath, Optional.empty(), Optional.empty()))
                .hasSameElementsAs(rangeClosed(0L, 13L).boxed().toList());
        assertThat(getCommitVersions(fileSystem, tablePath, Optional.of(0L), Optional.of(0L)))
                .hasSameElementsAs(rangeClosed(0L, 0L).boxed().toList());
        assertThat(getCommitVersions(fileSystem, tablePath, Optional.of(10L), Optional.empty()))
                .hasSameElementsAs(rangeClosed(10L, 13L).boxed().toList());
        assertThat(getCommitVersions(fileSystem, tablePath, Optional.of(10L), Optional.of(12L)))
                .hasSameElementsAs(rangeClosed(10L, 12L).boxed().toList());
        assertThat(getCommitVersions(fileSystem, tablePath, Optional.empty(), Optional.of(5L)))
                .hasSameElementsAs(rangeClosed(0L, 5L).boxed().toList());
        assertThat(getCommitVersions(fileSystem, tablePath, Optional.of(5L), Optional.of(5L)))
                .hasSameElementsAs(rangeClosed(5L, 5L).boxed().toList());

        assertThat(getCommitVersions(fileSystem, tablePath, Optional.empty(), Optional.of(14L)))
                .hasSameElementsAs(rangeClosed(0L, 13L).boxed().toList());

        assertThatThrownBy(() -> getCommitVersions(fileSystem, tablePath, Optional.of(6L), Optional.of(5L)))
                .hasMessage("startVersion must be less or equal than endVersion");
    }

    @Test
    public void testIteratorOnTableWithTransactionLogsRemoved()
            throws Exception
    {
        TrinoFileSystem fileSystem = new HdfsFileSystemFactory(HDFS_ENVIRONMENT).create(DeltaTestingConnectorSession.SESSION);
        URI resource = getClass().getClassLoader().getResource("databricks/delta_log_retention").toURI();
        Path tablePath = new Path(resource);

        assertEquals(
                stream(new DeltaLakeTransactionLogIterator(fileSystem, tablePath, Optional.empty(), Optional.empty()))
                        .collect(toImmutableList()).size(),
                3);
        assertThat(getCommitVersions(fileSystem, tablePath, Optional.empty(), Optional.empty()))
                .hasSameElementsAs(rangeClosed(3L, 5L).boxed().toList());
        assertThat(getCommitVersions(fileSystem, tablePath, Optional.of(1L), Optional.empty()))
                .hasSameElementsAs(rangeClosed(3L, 5L).boxed().toList());
        assertThat(getCommitVersions(fileSystem, tablePath, Optional.of(1L), Optional.of(3L)))
                .hasSameElementsAs(rangeClosed(3L, 3L).boxed().toList());
        // commit corresponding to the checkpoint
        assertThat(getCommitVersions(fileSystem, tablePath, Optional.of(4L), Optional.of(4L)))
                .hasSameElementsAs(rangeClosed(4L, 4L).boxed().toList());
        // commit following the checkpoint
        assertThat(getCommitVersions(fileSystem, tablePath, Optional.of(5L), Optional.of(5L)))
                .hasSameElementsAs(rangeClosed(5L, 5L).boxed().toList());
        assertThat(getCommitVersions(fileSystem, tablePath, Optional.of(1L), Optional.of(2L)))
                .isEmpty();

        assertThat(getCommitVersions(fileSystem, tablePath, Optional.empty(), Optional.of(6L)))
                .hasSameElementsAs(rangeClosed(3L, 5L).boxed().toList());
    }

    private static List<Long> getCommitVersions(TrinoFileSystem fileSystem, Path tablePath, Optional<Long> startVersion, Optional<Long> endVersion)
    {
        return stream(new DeltaLakeTransactionLogIterator(fileSystem, tablePath, startVersion, endVersion))
                .flatMap(Collection::stream)
                .map(DeltaLakeTransactionLogEntry::getCommitInfo)
                .filter(Objects::nonNull)
                .map(CommitInfoEntry::getVersion)
                .collect(toImmutableList());
    }
}
