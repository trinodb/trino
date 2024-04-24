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
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.memory.MemoryFileSystem;
import io.trino.plugin.hive.util.AcidTables.AcidState;
import io.trino.plugin.hive.util.AcidTables.ParsedBase;
import io.trino.plugin.hive.util.AcidTables.ParsedDelta;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import static io.trino.plugin.hive.util.AcidTables.deleteDeltaSubdir;
import static io.trino.plugin.hive.util.AcidTables.getAcidState;
import static io.trino.plugin.hive.util.AcidTables.parseBase;
import static io.trino.plugin.hive.util.AcidTables.parseDelta;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * See TestHiveAcidUtils for equivalent tests against Hive AcidUtils from the Hive codebase.
 */
public class TestAcidTables
{
    private static final byte[] FAKE_DATA = {65, 66, 67};

    @Test
    public void testParseBase()
    {
        ParsedBase base = parseBase("base_000123");
        assertThat(base.writeId()).isEqualTo(123);
        assertThat(base.visibilityId()).isEqualTo(0);

        base = parseBase("base_123_v456");
        assertThat(base.writeId()).isEqualTo(123);
        assertThat(base.visibilityId()).isEqualTo(456);
    }

    @Test
    public void testParseDelta()
    {
        ParsedDelta delta;
        delta = parseDelta("/tbl/part1/delta_12_34", "delta_", List.of());
        assertThat(12).isEqualTo(delta.min());
        assertThat(34).isEqualTo(delta.max());
        assertThat(-1).isEqualTo(delta.statementId());

        delta = parseDelta("/tbl/part1/delete_delta_12_34", "delete_delta_", List.of());
        assertThat(12).isEqualTo(delta.min());
        assertThat(34).isEqualTo(delta.max());
        assertThat(-1).isEqualTo(delta.statementId());

        delta = parseDelta("/tbl/part1/delta_12_34_56", "delta_", List.of());
        assertThat(12).isEqualTo(delta.min());
        assertThat(34).isEqualTo(delta.max());
        assertThat(56).isEqualTo(delta.statementId());

        delta = parseDelta("/tbl/part1/delete_delta_12_34_56", "delete_delta_", List.of());
        assertThat(12).isEqualTo(delta.min());
        assertThat(34).isEqualTo(delta.max());
        assertThat(56).isEqualTo(delta.statementId());

        // v78 visibility part should be ignored

        delta = parseDelta("/tbl/part1/delta_12_34_v78", "delta_", List.of());
        assertThat(12).isEqualTo(delta.min());
        assertThat(34).isEqualTo(delta.max());
        assertThat(-1).isEqualTo(delta.statementId());

        delta = parseDelta("/tbl/part1/delete_delta_12_34_v78", "delete_delta_", List.of());
        assertThat(12).isEqualTo(delta.min());
        assertThat(34).isEqualTo(delta.max());
        assertThat(-1).isEqualTo(delta.statementId());

        delta = parseDelta("/tbl/part1/delta_12_34_56_v78", "delta_", List.of());
        assertThat(12).isEqualTo(delta.min());
        assertThat(34).isEqualTo(delta.max());
        assertThat(56).isEqualTo(delta.statementId());

        delta = parseDelta("/tbl/part1/delete_delta_12_34_56_v78", "delete_delta_", List.of());
        assertThat(12).isEqualTo(delta.min());
        assertThat(34).isEqualTo(delta.max());
        assertThat(56).isEqualTo(delta.statementId());
    }

    @Test
    public void testOriginal()
            throws Exception
    {
        TrinoFileSystem fileSystem = new MemoryFileSystem();
        createFile(fileSystem, "memory:///tbl/part1/000000_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/000000_0_copy_1", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/000000_0_copy_2", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/000001_1", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/000002_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/random", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/_done", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/_tmp/000000_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/_tmp/abc/000000_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/subdir/000000_0", FAKE_DATA);

        AcidState state = getAcidState(
                fileSystem,
                Location.of("memory:///tbl/part1"),
                new ValidWriteIdList("tbl:100:%d:".formatted(Long.MAX_VALUE)));

        assertThat(state.baseDirectory()).isEmpty();
        assertThat(state.deltas()).isEmpty();

        List<FileEntry> files = state.originalFiles();
        assertThat(files.size()).isEqualTo(7);
        assertThat(files.get(0).location()).isEqualTo(Location.of("memory:///tbl/part1/000000_0"));
        assertThat(files.get(1).location()).isEqualTo(Location.of("memory:///tbl/part1/000000_0_copy_1"));
        assertThat(files.get(2).location()).isEqualTo(Location.of("memory:///tbl/part1/000000_0_copy_2"));
        assertThat(files.get(3).location()).isEqualTo(Location.of("memory:///tbl/part1/000001_1"));
        assertThat(files.get(4).location()).isEqualTo(Location.of("memory:///tbl/part1/000002_0"));
        assertThat(files.get(5).location()).isEqualTo(Location.of("memory:///tbl/part1/random"));
        assertThat(files.get(6).location()).isEqualTo(Location.of("memory:///tbl/part1/subdir/000000_0"));
    }

    @Test
    public void testOriginalDeltas()
            throws Exception
    {
        TrinoFileSystem fileSystem = new MemoryFileSystem();
        createFile(fileSystem, "memory:///tbl/part1/000000_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/000001_1", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/000002_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/random", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/_done", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/_tmp/000000_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/_tmp/delta_025_025/000000_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/subdir/000000_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_025_025/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_029_029/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_025_030/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_050_100/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_101_101/bucket_0", FAKE_DATA);

        AcidState state = getAcidState(
                fileSystem,
                Location.of("memory:///tbl/part1"),
                new ValidWriteIdList("tbl:100:%d:".formatted(Long.MAX_VALUE)));

        assertThat(state.baseDirectory()).isEmpty();

        List<FileEntry> files = state.originalFiles();
        assertThat(files.size()).isEqualTo(5);
        assertThat(files.get(0).location()).isEqualTo(Location.of("memory:///tbl/part1/000000_0"));
        assertThat(files.get(1).location()).isEqualTo(Location.of("memory:///tbl/part1/000001_1"));
        assertThat(files.get(2).location()).isEqualTo(Location.of("memory:///tbl/part1/000002_0"));
        assertThat(files.get(3).location()).isEqualTo(Location.of("memory:///tbl/part1/random"));
        assertThat(files.get(4).location()).isEqualTo(Location.of("memory:///tbl/part1/subdir/000000_0"));

        List<ParsedDelta> deltas = state.deltas();
        assertThat(deltas.size()).isEqualTo(2);
        ParsedDelta delta = deltas.get(0);
        assertThat(delta.path()).isEqualTo("memory:///tbl/part1/delta_025_030");
        assertThat(delta.min()).isEqualTo(25);
        assertThat(delta.max()).isEqualTo(30);
        delta = deltas.get(1);
        assertThat(delta.path()).isEqualTo("memory:///tbl/part1/delta_050_100");
        assertThat(delta.min()).isEqualTo(50);
        assertThat(delta.max()).isEqualTo(100);
    }

    @Test
    public void testBaseDeltas()
            throws Exception
    {
        TrinoFileSystem fileSystem = new MemoryFileSystem();
        createFile(fileSystem, "memory:///tbl/part1/_tmp/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/_tmp/base_5/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/base_5/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/base_10/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/base_49/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_025_025/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_029_029/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_025_030/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_050_105/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_90_120/bucket_0", FAKE_DATA);

        AcidState dir = getAcidState(
                fileSystem,
                Location.of("memory:///tbl/part1"),
                new ValidWriteIdList("tbl:100:%d:".formatted(Long.MAX_VALUE)));

        assertThat(dir.baseDirectory()).contains(Location.of("memory:///tbl/part1/base_49"));
        assertThat(dir.originalFiles().size()).isEqualTo(0);

        List<ParsedDelta> deltas = dir.deltas();
        assertThat(deltas.size()).isEqualTo(1);
        ParsedDelta delta = deltas.get(0);
        assertThat(delta.path()).isEqualTo("memory:///tbl/part1/delta_050_105");
        assertThat(delta.min()).isEqualTo(50);
        assertThat(delta.max()).isEqualTo(105);
    }

    @Test
    public void testObsoleteOriginals()
            throws Exception
    {
        TrinoFileSystem fileSystem = new MemoryFileSystem();
        createFile(fileSystem, "memory:///tbl/part1/base_10/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/base_5/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/000000_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/000001_1", FAKE_DATA);

        AcidState state = getAcidState(
                fileSystem,
                Location.of("memory:///tbl/part1"),
                new ValidWriteIdList("tbl:150:%d:".formatted(Long.MAX_VALUE)));

        assertThat(state.baseDirectory()).contains(Location.of("memory:///tbl/part1/base_10"));
    }

    @Test
    public void testOverlapingDelta()
            throws Exception
    {
        TrinoFileSystem fileSystem = new MemoryFileSystem();
        createFile(fileSystem, "memory:///tbl/part1/delta_0000063_63/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_000062_62/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_00061_61/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_40_60/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_0060_60/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_052_55/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/base_50/bucket_0", FAKE_DATA);

        AcidState state = getAcidState(
                fileSystem,
                Location.of("memory:///tbl/part1"),
                new ValidWriteIdList("tbl:100:%d:".formatted(Long.MAX_VALUE)));

        assertThat(state.baseDirectory()).contains(Location.of("memory:///tbl/part1/base_50"));

        List<ParsedDelta> deltas = state.deltas();
        assertThat(deltas.size()).isEqualTo(4);
        assertThat(deltas.get(0).path()).isEqualTo("memory:///tbl/part1/delta_40_60");
        assertThat(deltas.get(1).path()).isEqualTo("memory:///tbl/part1/delta_00061_61");
        assertThat(deltas.get(2).path()).isEqualTo("memory:///tbl/part1/delta_000062_62");
        assertThat(deltas.get(3).path()).isEqualTo("memory:///tbl/part1/delta_0000063_63");
    }

    @Test
    public void testOverlapingDelta2()
            throws Exception
    {
        TrinoFileSystem fileSystem = new MemoryFileSystem();
        createFile(fileSystem, "memory:///tbl/part1/delta_0000063_63_0/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_000062_62_0/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_000062_62_3/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_00061_61_0/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_40_60/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_0060_60_1/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_0060_60_4/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_0060_60_7/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_052_55/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_058_58/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/base_50/bucket_0", FAKE_DATA);

        AcidState state = getAcidState(
                fileSystem,
                Location.of("memory:///tbl/part1"),
                new ValidWriteIdList("tbl:100:%d:".formatted(Long.MAX_VALUE)));

        assertThat(state.baseDirectory()).contains(Location.of("memory:///tbl/part1/base_50"));

        List<ParsedDelta> deltas = state.deltas();
        assertThat(deltas.size()).isEqualTo(5);
        assertThat(deltas.get(0).path()).isEqualTo("memory:///tbl/part1/delta_40_60");
        assertThat(deltas.get(1).path()).isEqualTo("memory:///tbl/part1/delta_00061_61_0");
        assertThat(deltas.get(2).path()).isEqualTo("memory:///tbl/part1/delta_000062_62_0");
        assertThat(deltas.get(3).path()).isEqualTo("memory:///tbl/part1/delta_000062_62_3");
        assertThat(deltas.get(4).path()).isEqualTo("memory:///tbl/part1/delta_0000063_63_0");
    }

    @Test
    public void deltasWithOpenTxnInRead()
            throws Exception
    {
        TrinoFileSystem fileSystem = new MemoryFileSystem();
        createFile(fileSystem, "memory:///tbl/part1/delta_1_1/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_2_5/bucket_0", FAKE_DATA);

        AcidState state = getAcidState(
                fileSystem,
                Location.of("memory:///tbl/part1"),
                new ValidWriteIdList("tbl:100:4:4"));

        List<ParsedDelta> deltas = state.deltas();
        assertThat(deltas.size()).isEqualTo(2);
        assertThat(deltas.get(0).path()).isEqualTo("memory:///tbl/part1/delta_1_1");
        assertThat(deltas.get(1).path()).isEqualTo("memory:///tbl/part1/delta_2_5");
    }

    @Test
    public void deltasWithOpenTxnInRead2()
            throws Exception
    {
        TrinoFileSystem fileSystem = new MemoryFileSystem();
        createFile(fileSystem, "memory:///tbl/part1/delta_1_1/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_2_5/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_4_4_1/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_4_4_3/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_101_101_1/bucket_0", FAKE_DATA);

        AcidState state = getAcidState(
                fileSystem,
                Location.of("memory:///tbl/part1"),
                new ValidWriteIdList("tbl:100:4:4"));

        List<ParsedDelta> deltas = state.deltas();
        assertThat(deltas.size()).isEqualTo(2);
        assertThat(deltas.get(0).path()).isEqualTo("memory:///tbl/part1/delta_1_1");
        assertThat(deltas.get(1).path()).isEqualTo("memory:///tbl/part1/delta_2_5");
    }

    @Test
    public void testBaseWithDeleteDeltas()
            throws Exception
    {
        TrinoFileSystem fileSystem = new MemoryFileSystem();
        createFile(fileSystem, "memory:///tbl/part1/base_5/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/base_10/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/base_49/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_025_025/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_029_029/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delete_delta_029_029/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_025_030/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delete_delta_025_030/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_050_105/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delete_delta_050_105/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delete_delta_110_110/bucket_0", FAKE_DATA);

        AcidState state = getAcidState(
                fileSystem,
                Location.of("memory:///tbl/part1"),
                new ValidWriteIdList("tbl:100:%d:".formatted(Long.MAX_VALUE)));

        assertThat(state.baseDirectory()).contains(Location.of("memory:///tbl/part1/base_49"));
        assertThat(state.originalFiles()).isEmpty();

        List<ParsedDelta> deltas = state.deltas();
        assertThat(deltas.size()).isEqualTo(2);
        assertThat(deltas.get(0).path()).isEqualTo("memory:///tbl/part1/delete_delta_050_105");
        assertThat(deltas.get(1).path()).isEqualTo("memory:///tbl/part1/delta_050_105");
        // The delete_delta_110_110 should not be read because it is greater than the high watermark.
    }

    @Test
    public void testOverlapingDeltaAndDeleteDelta()
            throws Exception
    {
        TrinoFileSystem fileSystem = new MemoryFileSystem();
        createFile(fileSystem, "memory:///tbl/part1/delta_0000063_63/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_000062_62/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_00061_61/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delete_delta_00064_64/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_40_60/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delete_delta_40_60/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_0060_60/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_052_55/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delete_delta_052_55/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/base_50/bucket_0", FAKE_DATA);

        AcidState state = getAcidState(
                fileSystem,
                Location.of("memory:///tbl/part1"),
                new ValidWriteIdList("tbl:100:%d:".formatted(Long.MAX_VALUE)));

        assertThat(state.baseDirectory()).contains(Location.of("memory:///tbl/part1/base_50"));

        List<ParsedDelta> deltas = state.deltas();
        assertThat(deltas.size()).isEqualTo(6);
        assertThat(deltas.get(0).path()).isEqualTo("memory:///tbl/part1/delete_delta_40_60");
        assertThat(deltas.get(1).path()).isEqualTo("memory:///tbl/part1/delta_40_60");
        assertThat(deltas.get(2).path()).isEqualTo("memory:///tbl/part1/delta_00061_61");
        assertThat(deltas.get(3).path()).isEqualTo("memory:///tbl/part1/delta_000062_62");
        assertThat(deltas.get(4).path()).isEqualTo("memory:///tbl/part1/delta_0000063_63");
        assertThat(deltas.get(5).path()).isEqualTo("memory:///tbl/part1/delete_delta_00064_64");
    }

    @Test
    public void testMinorCompactedDeltaMakesInBetweenDelteDeltaObsolete()
            throws Exception
    {
        // This test checks that if we have a minor compacted delta for the txn range [40,60]
        // then it will make any delete delta in that range as obsolete.
        TrinoFileSystem fileSystem = new MemoryFileSystem();
        createFile(fileSystem, "memory:///tbl/part1/delta_40_60/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delete_delta_50_50/bucket_0", FAKE_DATA);

        AcidState state = getAcidState(
                fileSystem,
                Location.of("memory:///tbl/part1"),
                new ValidWriteIdList("tbl:100:%d:".formatted(Long.MAX_VALUE)));

        List<ParsedDelta> deltas = state.deltas();
        assertThat(deltas.size()).isEqualTo(1);
        assertThat(deltas.get(0).path()).isEqualTo("memory:///tbl/part1/delta_40_60");
    }

    @Test
    public void deleteDeltasWithOpenTxnInRead()
            throws Exception
    {
        TrinoFileSystem fileSystem = new MemoryFileSystem();
        createFile(fileSystem, "memory:///tbl/part1/delta_1_1/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_2_5/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delete_delta_2_5/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delete_delta_3_3/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_4_4_1/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_4_4_3/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_101_101_1/bucket_0", FAKE_DATA);

        AcidState state = getAcidState(
                fileSystem,
                Location.of("memory:///tbl/part1"),
                new ValidWriteIdList("tbl:100:4:4"));

        List<ParsedDelta> deltas = state.deltas();
        assertThat(deltas.size()).isEqualTo(3);
        assertThat(deltas.get(0).path()).isEqualTo("memory:///tbl/part1/delta_1_1");
        assertThat(deltas.get(1).path()).isEqualTo("memory:///tbl/part1/delete_delta_2_5");
        assertThat(deltas.get(2).path()).isEqualTo("memory:///tbl/part1/delta_2_5");
        // Note that delete_delta_3_3 should not be read, when a minor compacted
        // [delete_]delta_2_5 is present.
    }

    @Test
    public void testDeleteDeltaSubdirPathGeneration()
    {
        String deleteDeltaSubdirPath = deleteDeltaSubdir(13, 5);
        assertThat(deleteDeltaSubdirPath).isEqualTo("delete_delta_0000013_0000013_0005");
    }

    @Test
    public void testSkippingSubDirectories()
            throws IOException
    {
        TrinoFileSystem fileSystem = new MemoryFileSystem();
        createFile(fileSystem, "memory:///tbl/part1/base_1/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/base_1/base_1/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_025_025/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delta_025_025/delta_025_025/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delete_delta_029_029/bucket_0", FAKE_DATA);
        createFile(fileSystem, "memory:///tbl/part1/delete_delta_029_029/delete_delta_029_029/bucket_0", FAKE_DATA);

        AcidState state = getAcidState(
                fileSystem,
                Location.of("memory:///tbl/part1"),
                new ValidWriteIdList("tbl:100:%d:".formatted(Long.MAX_VALUE)));

        // Subdirectories in base directory should be skipped similar to Hive implementation
        assertThat(state.baseDirectory()).contains(Location.of("memory:///tbl/part1/base_1"));
        assertThat(state.originalFiles()).isEmpty();

        // Subdirectories in delta directories should be skipped similar to Hive implementation
        List<ParsedDelta> deltas = state.deltas();
        assertThat(deltas.size()).isEqualTo(2);
        assertThat(deltas.get(0).path()).isEqualTo("memory:///tbl/part1/delta_025_025");
        assertThat(deltas.get(1).path()).isEqualTo("memory:///tbl/part1/delete_delta_029_029");
    }

    private static void createFile(TrinoFileSystem fileSystem, String location, byte[] data)
            throws IOException
    {
        try (OutputStream outputStream = fileSystem.newOutputFile(Location.of(location)).create()) {
            outputStream.write(data);
        }
    }
}
