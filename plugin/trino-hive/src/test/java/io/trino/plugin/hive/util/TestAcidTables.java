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
package io.trino.plugin.hive.util;

import io.trino.filesystem.FileEntry;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfiguration;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.util.AcidTables.AcidState;
import io.trino.plugin.hive.util.AcidTables.ParsedBase;
import io.trino.plugin.hive.util.AcidTables.ParsedDelta;
import io.trino.plugin.hive.util.FileSystemTesting.MockFile;
import io.trino.plugin.hive.util.FileSystemTesting.MockFileSystem;
import io.trino.plugin.hive.util.FileSystemTesting.MockPath;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import java.util.List;

import static io.trino.hadoop.ConfigurationInstantiator.newEmptyConfiguration;
import static io.trino.plugin.hive.util.AcidTables.deleteDeltaSubdir;
import static io.trino.plugin.hive.util.AcidTables.getAcidState;
import static io.trino.plugin.hive.util.AcidTables.parseBase;
import static io.trino.plugin.hive.util.AcidTables.parseDelta;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestAcidTables
{
    private static final byte[] FAKE_DATA = {65, 66, 67};

    @Test
    public void testParseBase()
    {
        ParsedBase base = parseBase("base_000123");
        assertEquals(base.writeId(), 123);
        assertEquals(base.visibilityId(), 0);

        base = parseBase("base_123_v456");
        assertEquals(base.writeId(), 123);
        assertEquals(base.visibilityId(), 456);
    }

    @Test
    public void testParseDelta()
    {
        ParsedDelta delta;
        delta = parseDelta("/tbl/part1/delta_12_34", "delta_", List.of());
        assertEquals(12, delta.min());
        assertEquals(34, delta.max());
        assertEquals(-1, delta.statementId());

        delta = parseDelta("/tbl/part1/delete_delta_12_34", "delete_delta_", List.of());
        assertEquals(12, delta.min());
        assertEquals(34, delta.max());
        assertEquals(-1, delta.statementId());

        delta = parseDelta("/tbl/part1/delta_12_34_56", "delta_", List.of());
        assertEquals(12, delta.min());
        assertEquals(34, delta.max());
        assertEquals(56, delta.statementId());

        delta = parseDelta("/tbl/part1/delete_delta_12_34_56", "delete_delta_", List.of());
        assertEquals(12, delta.min());
        assertEquals(34, delta.max());
        assertEquals(56, delta.statementId());

        // v78 visibility part should be ignored

        delta = parseDelta("/tbl/part1/delta_12_34_v78", "delta_", List.of());
        assertEquals(12, delta.min());
        assertEquals(34, delta.max());
        assertEquals(-1, delta.statementId());

        delta = parseDelta("/tbl/part1/delete_delta_12_34_v78", "delete_delta_", List.of());
        assertEquals(12, delta.min());
        assertEquals(34, delta.max());
        assertEquals(-1, delta.statementId());

        delta = parseDelta("/tbl/part1/delta_12_34_56_v78", "delta_", List.of());
        assertEquals(12, delta.min());
        assertEquals(34, delta.max());
        assertEquals(56, delta.statementId());

        delta = parseDelta("/tbl/part1/delete_delta_12_34_56_v78", "delete_delta_", List.of());
        assertEquals(12, delta.min());
        assertEquals(34, delta.max());
        assertEquals(56, delta.statementId());
    }

    @Test
    public void testOriginal()
            throws Exception
    {
        MockFileSystem fs = new MockFileSystem(newEmptyConfiguration(),
                new MockFile("mock:/tbl/part1/000000_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/000000_0_copy_1", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/000000_0_copy_2", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/000001_1", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/000002_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/random", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/_done", 0, FAKE_DATA),
                new MockFile("mock:/tbl/part1/subdir/000000_0", 0, FAKE_DATA));
        AcidState state = getAcidState(
                testingTrinoFileSystem(fs),
                new MockPath(fs, "mock:/tbl/part1").toString(),
                new ValidWriteIdList("tbl:100:%d:".formatted(Long.MAX_VALUE)));

        assertThat(state.baseDirectory()).isEmpty();
        assertThat(state.deltas()).isEmpty();

        List<FileEntry> files = state.originalFiles();
        assertEquals(files.size(), 7);
        assertEquals(files.get(0).path(), "mock:/tbl/part1/000000_0");
        assertEquals(files.get(1).path(), "mock:/tbl/part1/000000_0_copy_1");
        assertEquals(files.get(2).path(), "mock:/tbl/part1/000000_0_copy_2");
        assertEquals(files.get(3).path(), "mock:/tbl/part1/000001_1");
        assertEquals(files.get(4).path(), "mock:/tbl/part1/000002_0");
        assertEquals(files.get(5).path(), "mock:/tbl/part1/random");
        assertEquals(files.get(6).path(), "mock:/tbl/part1/subdir/000000_0");
    }

    @Test
    public void testOriginalDeltas()
            throws Exception
    {
        MockFileSystem fs = new MockFileSystem(newEmptyConfiguration(),
                new MockFile("mock:/tbl/part1/000000_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/000001_1", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/000002_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/random", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/_done", 0, FAKE_DATA),
                new MockFile("mock:/tbl/part1/subdir/000000_0", 0, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_025_025/bucket_0", 0, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_029_029/bucket_0", 0, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_025_030/bucket_0", 0, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_050_100/bucket_0", 0, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_101_101/bucket_0", 0, FAKE_DATA));
        AcidState state = getAcidState(
                testingTrinoFileSystem(fs),
                new MockPath(fs, "mock:/tbl/part1").toString(),
                new ValidWriteIdList("tbl:100:%d:".formatted(Long.MAX_VALUE)));

        assertThat(state.baseDirectory()).isEmpty();

        List<FileEntry> files = state.originalFiles();
        assertEquals(files.size(), 5);
        assertEquals(files.get(0).path(), "mock:/tbl/part1/000000_0");
        assertEquals(files.get(1).path(), "mock:/tbl/part1/000001_1");
        assertEquals(files.get(2).path(), "mock:/tbl/part1/000002_0");
        assertEquals(files.get(3).path(), "mock:/tbl/part1/random");
        assertEquals(files.get(4).path(), "mock:/tbl/part1/subdir/000000_0");

        List<ParsedDelta> deltas = state.deltas();
        assertEquals(deltas.size(), 2);
        ParsedDelta delta = deltas.get(0);
        assertEquals(delta.path(), "mock:/tbl/part1/delta_025_030");
        assertEquals(delta.min(), 25);
        assertEquals(delta.max(), 30);
        delta = deltas.get(1);
        assertEquals(delta.path(), "mock:/tbl/part1/delta_050_100");
        assertEquals(delta.min(), 50);
        assertEquals(delta.max(), 100);
    }

    @Test
    public void testBaseDeltas()
            throws Exception
    {
        MockFileSystem fs = new MockFileSystem(newEmptyConfiguration(),
                new MockFile("mock:/tbl/part1/base_5/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/base_10/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/base_49/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_025_025/bucket_0", 0, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_029_029/bucket_0", 0, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_025_030/bucket_0", 0, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_050_105/bucket_0", 0, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_90_120/bucket_0", 0, FAKE_DATA));
        AcidState dir = getAcidState(
                testingTrinoFileSystem(fs),
                new MockPath(fs, "mock:/tbl/part1").toString(),
                new ValidWriteIdList("tbl:100:%d:".formatted(Long.MAX_VALUE)));

        assertThat(dir.baseDirectory()).contains("mock:/tbl/part1/base_49");
        assertEquals(dir.originalFiles().size(), 0);

        List<ParsedDelta> deltas = dir.deltas();
        assertEquals(deltas.size(), 1);
        ParsedDelta delta = deltas.get(0);
        assertEquals(delta.path(), "mock:/tbl/part1/delta_050_105");
        assertEquals(delta.min(), 50);
        assertEquals(delta.max(), 105);
    }

    @Test
    public void testObsoleteOriginals()
            throws Exception
    {
        MockFileSystem fs = new MockFileSystem(newEmptyConfiguration(),
                new MockFile("mock:/tbl/part1/base_10/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/base_5/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/000000_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/000001_1", 500, FAKE_DATA));
        AcidState state = getAcidState(
                testingTrinoFileSystem(fs),
                new MockPath(fs, "mock:/tbl/part1").toString(),
                new ValidWriteIdList("tbl:150:%d:".formatted(Long.MAX_VALUE)));

        assertThat(state.baseDirectory()).contains("mock:/tbl/part1/base_10");
    }

    @Test
    public void testOverlapingDelta()
            throws Exception
    {
        MockFileSystem fs = new MockFileSystem(newEmptyConfiguration(),
                new MockFile("mock:/tbl/part1/delta_0000063_63/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_000062_62/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_00061_61/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_40_60/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_0060_60/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_052_55/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/base_50/bucket_0", 500, FAKE_DATA));
        AcidState state = getAcidState(
                testingTrinoFileSystem(fs),
                new MockPath(fs, "mock:/tbl/part1").toString(),
                new ValidWriteIdList("tbl:100:%d:".formatted(Long.MAX_VALUE)));

        assertThat(state.baseDirectory()).contains("mock:/tbl/part1/base_50");

        List<ParsedDelta> deltas = state.deltas();
        assertEquals(deltas.size(), 4);
        assertEquals(deltas.get(0).path(), "mock:/tbl/part1/delta_40_60");
        assertEquals(deltas.get(1).path(), "mock:/tbl/part1/delta_00061_61");
        assertEquals(deltas.get(2).path(), "mock:/tbl/part1/delta_000062_62");
        assertEquals(deltas.get(3).path(), "mock:/tbl/part1/delta_0000063_63");
    }

    @Test
    public void testOverlapingDelta2()
            throws Exception
    {
        MockFileSystem fs = new MockFileSystem(newEmptyConfiguration(),
                new MockFile("mock:/tbl/part1/delta_0000063_63_0/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_000062_62_0/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_000062_62_3/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_00061_61_0/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_40_60/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_0060_60_1/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_0060_60_4/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_0060_60_7/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_052_55/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_058_58/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/base_50/bucket_0", 500, FAKE_DATA));
        AcidState state = getAcidState(
                testingTrinoFileSystem(fs),
                new MockPath(fs, "mock:/tbl/part1").toString(),
                new ValidWriteIdList("tbl:100:%d:".formatted(Long.MAX_VALUE)));

        assertThat(state.baseDirectory()).contains("mock:/tbl/part1/base_50");

        List<ParsedDelta> deltas = state.deltas();
        assertEquals(deltas.size(), 5);
        assertEquals(deltas.get(0).path(), "mock:/tbl/part1/delta_40_60");
        assertEquals(deltas.get(1).path(), "mock:/tbl/part1/delta_00061_61_0");
        assertEquals(deltas.get(2).path(), "mock:/tbl/part1/delta_000062_62_0");
        assertEquals(deltas.get(3).path(), "mock:/tbl/part1/delta_000062_62_3");
        assertEquals(deltas.get(4).path(), "mock:/tbl/part1/delta_0000063_63_0");
    }

    @Test
    public void deltasWithOpenTxnInRead()
            throws Exception
    {
        MockFileSystem fs = new MockFileSystem(newEmptyConfiguration(),
                new MockFile("mock:/tbl/part1/delta_1_1/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_2_5/bucket_0", 500, FAKE_DATA));
        AcidState state = getAcidState(
                testingTrinoFileSystem(fs),
                new MockPath(fs, "mock:/tbl/part1").toString(),
                new ValidWriteIdList("tbl:100:4:4"));

        List<ParsedDelta> deltas = state.deltas();
        assertEquals(deltas.size(), 2);
        assertEquals(deltas.get(0).path(), "mock:/tbl/part1/delta_1_1");
        assertEquals(deltas.get(1).path(), "mock:/tbl/part1/delta_2_5");
    }

    @Test
    public void deltasWithOpenTxnInRead2()
            throws Exception
    {
        MockFileSystem fs = new MockFileSystem(newEmptyConfiguration(),
                new MockFile("mock:/tbl/part1/delta_1_1/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_2_5/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_4_4_1/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_4_4_3/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_101_101_1/bucket_0", 500, FAKE_DATA));
        AcidState state = getAcidState(
                testingTrinoFileSystem(fs),
                new MockPath(fs, "mock:/tbl/part1").toString(),
                new ValidWriteIdList("tbl:100:4:4"));

        List<ParsedDelta> deltas = state.deltas();
        assertEquals(deltas.size(), 2);
        assertEquals(deltas.get(0).path(), "mock:/tbl/part1/delta_1_1");
        assertEquals(deltas.get(1).path(), "mock:/tbl/part1/delta_2_5");
    }

    @Test
    public void testBaseWithDeleteDeltas()
            throws Exception
    {
        MockFileSystem fs = new MockFileSystem(newEmptyConfiguration(),
                new MockFile("mock:/tbl/part1/base_5/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/base_10/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/base_49/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_025_025/bucket_0", 0, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_029_029/bucket_0", 0, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delete_delta_029_029/bucket_0", 0, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_025_030/bucket_0", 0, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delete_delta_025_030/bucket_0", 0, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_050_105/bucket_0", 0, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delete_delta_050_105/bucket_0", 0, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delete_delta_110_110/bucket_0", 0, FAKE_DATA));
        AcidState state = getAcidState(
                testingTrinoFileSystem(fs),
                new MockPath(fs, "mock:/tbl/part1").toString(),
                new ValidWriteIdList("tbl:100:%d:".formatted(Long.MAX_VALUE)));

        assertThat(state.baseDirectory()).contains("mock:/tbl/part1/base_49");
        assertThat(state.originalFiles()).isEmpty();

        List<ParsedDelta> deltas = state.deltas();
        assertEquals(deltas.size(), 2);
        assertEquals(deltas.get(0).path(), "mock:/tbl/part1/delete_delta_050_105");
        assertEquals(deltas.get(1).path(), "mock:/tbl/part1/delta_050_105");
        // The delete_delta_110_110 should not be read because it is greater than the high watermark.
    }

    @Test
    public void testOverlapingDeltaAndDeleteDelta()
            throws Exception
    {
        MockFileSystem fs = new MockFileSystem(newEmptyConfiguration(),
                new MockFile("mock:/tbl/part1/delta_0000063_63/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_000062_62/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_00061_61/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delete_delta_00064_64/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_40_60/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delete_delta_40_60/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_0060_60/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_052_55/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delete_delta_052_55/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/base_50/bucket_0", 500, FAKE_DATA));
        AcidState state = getAcidState(
                testingTrinoFileSystem(fs),
                new MockPath(fs, "mock:/tbl/part1").toString(),
                new ValidWriteIdList("tbl:100:%d:".formatted(Long.MAX_VALUE)));

        assertThat(state.baseDirectory()).contains("mock:/tbl/part1/base_50");

        List<ParsedDelta> deltas = state.deltas();
        assertEquals(deltas.size(), 6);
        assertEquals(deltas.get(0).path(), "mock:/tbl/part1/delete_delta_40_60");
        assertEquals(deltas.get(1).path(), "mock:/tbl/part1/delta_40_60");
        assertEquals(deltas.get(2).path(), "mock:/tbl/part1/delta_00061_61");
        assertEquals(deltas.get(3).path(), "mock:/tbl/part1/delta_000062_62");
        assertEquals(deltas.get(4).path(), "mock:/tbl/part1/delta_0000063_63");
        assertEquals(deltas.get(5).path(), "mock:/tbl/part1/delete_delta_00064_64");
    }

    @Test
    public void testMinorCompactedDeltaMakesInBetweenDelteDeltaObsolete()
            throws Exception
    {
        // This test checks that if we have a minor compacted delta for the txn range [40,60]
        // then it will make any delete delta in that range as obsolete.
        MockFileSystem fs = new MockFileSystem(newEmptyConfiguration(),
                new MockFile("mock:/tbl/part1/delta_40_60/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delete_delta_50_50/bucket_0", 500, FAKE_DATA));
        AcidState state = getAcidState(
                testingTrinoFileSystem(fs),
                new MockPath(fs, "mock:/tbl/part1").toString(),
                new ValidWriteIdList("tbl:100:%d:".formatted(Long.MAX_VALUE)));

        List<ParsedDelta> deltas = state.deltas();
        assertEquals(deltas.size(), 1);
        assertEquals(deltas.get(0).path(), "mock:/tbl/part1/delta_40_60");
    }

    @Test
    public void deleteDeltasWithOpenTxnInRead()
            throws Exception
    {
        MockFileSystem fs = new MockFileSystem(newEmptyConfiguration(),
                new MockFile("mock:/tbl/part1/delta_1_1/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_2_5/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delete_delta_2_5/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delete_delta_3_3/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_4_4_1/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_4_4_3/bucket_0", 500, FAKE_DATA),
                new MockFile("mock:/tbl/part1/delta_101_101_1/bucket_0", 500, FAKE_DATA));
        AcidState state = getAcidState(
                testingTrinoFileSystem(fs),
                new MockPath(fs, "mock:/tbl/part1").toString(),
                new ValidWriteIdList("tbl:100:4:4"));

        List<ParsedDelta> deltas = state.deltas();
        assertEquals(deltas.size(), 3);
        assertEquals(deltas.get(0).path(), "mock:/tbl/part1/delta_1_1");
        assertEquals(deltas.get(1).path(), "mock:/tbl/part1/delete_delta_2_5");
        assertEquals(deltas.get(2).path(), "mock:/tbl/part1/delta_2_5");
        // Note that delete_delta_3_3 should not be read, when a minor compacted
        // [delete_]delta_2_5 is present.
    }

    @Test
    public void testDeleteDeltaSubdirPathGeneration()
    {
        String deleteDeltaSubdirPath = deleteDeltaSubdir(13, 5);
        assertEquals(deleteDeltaSubdirPath, "delete_delta_0000013_0000013_0005");
    }

    private static TrinoFileSystem testingTrinoFileSystem(FileSystem fileSystem)
    {
        HdfsConfiguration hdfsConfiguration = (context, uri) -> newEmptyConfiguration();

        HdfsEnvironment environment = new HdfsEnvironment(hdfsConfiguration, new HdfsConfig(), new NoHdfsAuthentication())
        {
            @Override
            public FileSystem getFileSystem(ConnectorIdentity identity, Path path, Configuration configuration)
            {
                return fileSystem;
            }
        };

        ConnectorIdentity identity = ConnectorIdentity.forUser("test").build();
        return new HdfsFileSystemFactory(environment).create(identity);
    }
}
