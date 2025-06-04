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

import io.trino.plugin.hive.util.FileSystemTesting.MockFile;
import io.trino.plugin.hive.util.FileSystemTesting.MockFileSystem;
import io.trino.plugin.hive.util.FileSystemTesting.MockPath;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidCompactorWriteIdList;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.shims.HadoopShims.HdfsFileStatusWithId;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test the expected behavior of the Hive AcidUtils class from the Hive codebase.
 */
public class TestHiveAcidUtils
{
    @Test
    public void testParsing()
    {
        assertThat(AcidUtils.parseBase(new Path("/tmp/base_000123"))).isEqualTo(123);
    }

    @Test
    public void testOriginal()
            throws Exception
    {
        Configuration conf = new Configuration(false);
        MockFileSystem fs = new MockFileSystem(conf,
                new MockFile("mock:/tbl/part1/000000_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/000000_0" + Utilities.COPY_KEYWORD + "1", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/000000_0" + Utilities.COPY_KEYWORD + "2", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/000001_1", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/000002_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/random", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/_done", 0, new byte[0]),
                new MockFile("mock:/tbl/part1/_tmp/000000_0", 0, new byte[0]),
                new MockFile("mock:/tbl/part1/_tmp/abc/000000_0", 0, new byte[0]),
                new MockFile("mock:/tbl/part1/subdir/000000_0", 0, new byte[0]));
        AcidUtils.Directory dir = AcidUtils.getAcidState(
                new MockPath(fs, "/tbl/part1"),
                conf,
                new ValidReaderWriteIdList("tbl:100:" + Long.MAX_VALUE + ":"));
        assertThat(dir.getBaseDirectory()).isNull();
        assertThat(dir.getCurrentDirectories()).isEmpty();
        assertThat(dir.getObsolete()).isEmpty();
        List<HdfsFileStatusWithId> result = dir.getOriginalFiles();
        assertThat(result).hasSize(7);
        assertThat(result.get(0).getFileStatus().getPath().toString()).isEqualTo("mock:/tbl/part1/000000_0");
        assertThat(result.get(1).getFileStatus().getPath().toString()).isEqualTo("mock:/tbl/part1/000000_0" + Utilities.COPY_KEYWORD + "1");
        assertThat(result.get(2).getFileStatus().getPath().toString()).isEqualTo("mock:/tbl/part1/000000_0" + Utilities.COPY_KEYWORD + "2");
        assertThat(result.get(3).getFileStatus().getPath().toString()).isEqualTo("mock:/tbl/part1/000001_1");
        assertThat(result.get(4).getFileStatus().getPath().toString()).isEqualTo("mock:/tbl/part1/000002_0");
        assertThat(result.get(5).getFileStatus().getPath().toString()).isEqualTo("mock:/tbl/part1/random");
        assertThat(result.get(6).getFileStatus().getPath().toString()).isEqualTo("mock:/tbl/part1/subdir/000000_0");
    }

    @Test
    public void testOriginalDeltas()
            throws Exception
    {
        Configuration conf = new Configuration(false);
        MockFileSystem fs = new MockFileSystem(conf,
                new MockFile("mock:/tbl/part1/000000_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/000001_1", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/000002_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/random", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/_done", 0, new byte[0]),
                new MockFile("mock:/tbl/part1/_tmp/000000_0", 0, new byte[0]),
                new MockFile("mock:/tbl/part1/_tmp/delta_025_025/000000_0", 0, new byte[0]),
                new MockFile("mock:/tbl/part1/subdir/000000_0", 0, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_025_025/bucket_0", 0, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_029_029/bucket_0", 0, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_025_030/bucket_0", 0, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_050_100/bucket_0", 0, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_101_101/bucket_0", 0, new byte[0]));
        AcidUtils.Directory dir = AcidUtils.getAcidState(
                new MockPath(fs, "mock:/tbl/part1"),
                conf,
                new ValidReaderWriteIdList("tbl:100:" + Long.MAX_VALUE + ":"));
        assertThat(dir.getBaseDirectory()).isNull();
        List<FileStatus> obsolete = dir.getObsolete();
        assertThat(obsolete).hasSize(2);
        assertThat(obsolete.get(0).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_025_025");
        assertThat(obsolete.get(1).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_029_029");
        List<HdfsFileStatusWithId> result = dir.getOriginalFiles();
        assertThat(result).hasSize(5);
        assertThat(result.get(0).getFileStatus().getPath().toString()).isEqualTo("mock:/tbl/part1/000000_0");
        assertThat(result.get(1).getFileStatus().getPath().toString()).isEqualTo("mock:/tbl/part1/000001_1");
        assertThat(result.get(2).getFileStatus().getPath().toString()).isEqualTo("mock:/tbl/part1/000002_0");
        assertThat(result.get(3).getFileStatus().getPath().toString()).isEqualTo("mock:/tbl/part1/random");
        assertThat(result.get(4).getFileStatus().getPath().toString()).isEqualTo("mock:/tbl/part1/subdir/000000_0");
        List<AcidUtils.ParsedDelta> deltas = dir.getCurrentDirectories();
        assertThat(deltas).hasSize(2);
        AcidUtils.ParsedDelta delt = deltas.get(0);
        assertThat(delt.getPath().toString()).isEqualTo("mock:/tbl/part1/delta_025_030");
        assertThat(delt.getMinWriteId()).isEqualTo(25);
        assertThat(delt.getMaxWriteId()).isEqualTo(30);
        delt = deltas.get(1);
        assertThat(delt.getPath().toString()).isEqualTo("mock:/tbl/part1/delta_050_100");
        assertThat(delt.getMinWriteId()).isEqualTo(50);
        assertThat(delt.getMaxWriteId()).isEqualTo(100);
    }

    @Test
    public void testBaseDeltas()
            throws Exception
    {
        Configuration conf = new Configuration(false);
        MockFileSystem fs = new MockFileSystem(conf,
                new MockFile("mock:/tbl/part1/_tmp/bucket_0", 0, new byte[0]),
                new MockFile("mock:/tbl/part1/_tmp/base_5/bucket_0", 0, new byte[0]),
                new MockFile("mock:/tbl/part1/base_5/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/base_10/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/base_49/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_025_025/bucket_0", 0, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_029_029/bucket_0", 0, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_025_030/bucket_0", 0, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_050_105/bucket_0", 0, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_90_120/bucket_0", 0, new byte[0]));
        MockPath part = new MockPath(fs, "mock:/tbl/part1");
        AcidUtils.Directory dir = AcidUtils.getAcidState(part, conf, new ValidReaderWriteIdList("tbl:100:" + Long.MAX_VALUE + ":"));
        assertThat(dir.getBaseDirectory().toString()).isEqualTo("mock:/tbl/part1/base_49");
        List<FileStatus> obsolete = dir.getObsolete();
        assertThat(obsolete).hasSize(5);
        assertThat(obsolete.get(0).getPath().toString()).isEqualTo("mock:/tbl/part1/base_10");
        assertThat(obsolete.get(1).getPath().toString()).isEqualTo("mock:/tbl/part1/base_5");
        assertThat(obsolete.get(2).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_025_030");
        assertThat(obsolete.get(3).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_025_025");
        assertThat(obsolete.get(4).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_029_029");
        assertThat(dir.getOriginalFiles()).isEmpty();
        List<AcidUtils.ParsedDelta> deltas = dir.getCurrentDirectories();
        assertThat(deltas).hasSize(1);
        AcidUtils.ParsedDelta delt = deltas.get(0);
        assertThat(delt.getPath().toString()).isEqualTo("mock:/tbl/part1/delta_050_105");
        assertThat(delt.getMinWriteId()).isEqualTo(50);
        assertThat(delt.getMaxWriteId()).isEqualTo(105);
    }

    @Test
    public void testObsoleteOriginals()
            throws Exception
    {
        Configuration conf = new Configuration(false);
        MockFileSystem fs = new MockFileSystem(conf,
                new MockFile("mock:/tbl/part1/base_10/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/base_5/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/000000_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/000001_1", 500, new byte[0]));
        Path part = new MockPath(fs, "/tbl/part1");
        AcidUtils.Directory dir = AcidUtils.getAcidState(part, conf, new ValidReaderWriteIdList("tbl:150:" + Long.MAX_VALUE + ":"));
        // Obsolete list should include the two original bucket files, and the old base dir
        List<FileStatus> obsolete = dir.getObsolete();
        assertThat(obsolete).hasSize(3);
        assertThat(obsolete.get(0).getPath().toString()).isEqualTo("mock:/tbl/part1/base_5");
        assertThat(dir.getBaseDirectory().toString()).isEqualTo("mock:/tbl/part1/base_10");
    }

    @Test
    public void testOverlapingDelta()
            throws Exception
    {
        Configuration conf = new Configuration(false);
        MockFileSystem fs = new MockFileSystem(conf,
                new MockFile("mock:/tbl/part1/delta_0000063_63/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_000062_62/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_00061_61/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_40_60/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_0060_60/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_052_55/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/base_50/bucket_0", 500, new byte[0]));
        Path part = new MockPath(fs, "mock:/tbl/part1");
        AcidUtils.Directory dir = AcidUtils.getAcidState(part, conf, new ValidReaderWriteIdList("tbl:100:" + Long.MAX_VALUE + ":"));
        assertThat(dir.getBaseDirectory().toString()).isEqualTo("mock:/tbl/part1/base_50");
        List<FileStatus> obsolete = dir.getObsolete();
        assertThat(obsolete).hasSize(2);
        assertThat(obsolete.get(0).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_052_55");
        assertThat(obsolete.get(1).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_0060_60");
        List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
        assertThat(delts).hasSize(4);
        assertThat(delts.get(0).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_40_60");
        assertThat(delts.get(1).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_00061_61");
        assertThat(delts.get(2).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_000062_62");
        assertThat(delts.get(3).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_0000063_63");
    }

    @Test
    public void testOverlapingDelta2()
            throws Exception
    {
        Configuration conf = new Configuration(false);
        MockFileSystem fs = new MockFileSystem(conf,
                new MockFile("mock:/tbl/part1/delta_0000063_63_0/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_000062_62_0/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_000062_62_3/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_00061_61_0/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_40_60/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_0060_60_1/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_0060_60_4/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_0060_60_7/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_052_55/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_058_58/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/base_50/bucket_0", 500, new byte[0]));
        Path part = new MockPath(fs, "mock:/tbl/part1");
        AcidUtils.Directory dir = AcidUtils.getAcidState(part, conf, new ValidReaderWriteIdList("tbl:100:" + Long.MAX_VALUE + ":"));
        assertThat(dir.getBaseDirectory().toString()).isEqualTo("mock:/tbl/part1/base_50");
        List<FileStatus> obsolete = dir.getObsolete();
        assertThat(obsolete).hasSize(5);
        assertThat(obsolete.get(0).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_052_55");
        assertThat(obsolete.get(1).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_058_58");
        assertThat(obsolete.get(2).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_0060_60_1");
        assertThat(obsolete.get(3).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_0060_60_4");
        assertThat(obsolete.get(4).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_0060_60_7");
        List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
        assertThat(delts).hasSize(5);
        assertThat(delts.get(0).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_40_60");
        assertThat(delts.get(1).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_00061_61_0");
        assertThat(delts.get(2).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_000062_62_0");
        assertThat(delts.get(3).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_000062_62_3");
        assertThat(delts.get(4).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_0000063_63_0");
    }

    @Test
    public void deltasWithOpenTxnInRead()
            throws Exception
    {
        Configuration conf = new Configuration(false);
        MockFileSystem fs = new MockFileSystem(conf,
                new MockFile("mock:/tbl/part1/delta_1_1/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_2_5/bucket_0", 500, new byte[0]));
        Path part = new MockPath(fs, "mock:/tbl/part1");
        AcidUtils.Directory dir = AcidUtils.getAcidState(part, conf, new ValidReaderWriteIdList("tbl:100:4:4"));
        List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
        assertThat(delts).hasSize(2);
        assertThat(delts.get(0).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_1_1");
        assertThat(delts.get(1).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_2_5");
    }

    @Test
    public void deltasWithOpenTxnInRead2()
            throws Exception
    {
        Configuration conf = new Configuration(false);
        MockFileSystem fs = new MockFileSystem(conf,
                new MockFile("mock:/tbl/part1/delta_1_1/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_2_5/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_4_4_1/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_4_4_3/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_101_101_1/bucket_0", 500, new byte[0]));
        Path part = new MockPath(fs, "mock:/tbl/part1");
        AcidUtils.Directory dir = AcidUtils.getAcidState(part, conf, new ValidReaderWriteIdList("tbl:100:4:4"));
        List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
        assertThat(delts).hasSize(2);
        assertThat(delts.get(0).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_1_1");
        assertThat(delts.get(1).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_2_5");
    }

    @Test
    public void deltasWithOpenTxnsNotInCompact()
            throws Exception
    {
        Configuration conf = new Configuration(false);
        MockFileSystem fs = new MockFileSystem(conf,
                new MockFile("mock:/tbl/part1/delta_1_1/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_2_5/bucket_0", 500, new byte[0]));
        Path part = new MockPath(fs, "mock:/tbl/part1");
        AcidUtils.Directory dir = AcidUtils.getAcidState(part, conf, new ValidCompactorWriteIdList("tbl:4:" + Long.MAX_VALUE));
        List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
        assertThat(delts).hasSize(1);
        assertThat(delts.get(0).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_1_1");
    }

    @Test
    public void deltasWithOpenTxnsNotInCompact2()
            throws Exception
    {
        Configuration conf = new Configuration(false);
        MockFileSystem fs = new MockFileSystem(conf,
                new MockFile("mock:/tbl/part1/delta_1_1/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_2_5/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_2_5/bucket_0" + AcidUtils.DELTA_SIDE_FILE_SUFFIX, 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_6_10/bucket_0", 500, new byte[0]));
        Path part = new MockPath(fs, "mock:/tbl/part1");
        AcidUtils.Directory dir = AcidUtils.getAcidState(part, conf, new ValidCompactorWriteIdList("tbl:3:" + Long.MAX_VALUE));
        List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
        assertThat(delts).hasSize(1);
        assertThat(delts.get(0).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_1_1");
    }

    @Test
    public void testBaseWithDeleteDeltas()
            throws Exception
    {
        Configuration conf = new Configuration(false);
        MockFileSystem fs = new MockFileSystem(conf,
                new MockFile("mock:/tbl/part1/base_5/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/base_10/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/base_49/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_025_025/bucket_0", 0, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_029_029/bucket_0", 0, new byte[0]),
                new MockFile("mock:/tbl/part1/delete_delta_029_029/bucket_0", 0, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_025_030/bucket_0", 0, new byte[0]),
                new MockFile("mock:/tbl/part1/delete_delta_025_030/bucket_0", 0, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_050_105/bucket_0", 0, new byte[0]),
                new MockFile("mock:/tbl/part1/delete_delta_050_105/bucket_0", 0, new byte[0]),
                new MockFile("mock:/tbl/part1/delete_delta_110_110/bucket_0", 0, new byte[0]));
        MockPath part = new MockPath(fs, "mock:/tbl/part1");
        AcidUtils.Directory dir = AcidUtils.getAcidState(part, conf, new ValidReaderWriteIdList("tbl:100:" + Long.MAX_VALUE + ":"));
        assertThat(dir.getBaseDirectory().toString()).isEqualTo("mock:/tbl/part1/base_49");
        List<FileStatus> obsolete = dir.getObsolete();
        assertThat(obsolete).hasSize(7);
        assertThat(obsolete.get(0).getPath().toString()).isEqualTo("mock:/tbl/part1/base_10");
        assertThat(obsolete.get(1).getPath().toString()).isEqualTo("mock:/tbl/part1/base_5");
        assertThat(obsolete.get(2).getPath().toString()).isEqualTo("mock:/tbl/part1/delete_delta_025_030");
        assertThat(obsolete.get(3).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_025_030");
        assertThat(obsolete.get(4).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_025_025");
        assertThat(obsolete.get(5).getPath().toString()).isEqualTo("mock:/tbl/part1/delete_delta_029_029");
        assertThat(obsolete.get(6).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_029_029");
        assertThat(dir.getOriginalFiles()).isEmpty();
        List<AcidUtils.ParsedDelta> deltas = dir.getCurrentDirectories();
        assertThat(deltas).hasSize(2);
        assertThat(deltas.get(0).getPath().toString()).isEqualTo("mock:/tbl/part1/delete_delta_050_105");
        assertThat(deltas.get(1).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_050_105");
        // The delete_delta_110_110 should not be read because it is greater than the high watermark.
    }

    @Test
    public void testOverlapingDeltaAndDeleteDelta()
            throws Exception
    {
        Configuration conf = new Configuration(false);
        MockFileSystem fs = new MockFileSystem(conf,
                new MockFile("mock:/tbl/part1/delta_0000063_63/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_000062_62/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_00061_61/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delete_delta_00064_64/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_40_60/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delete_delta_40_60/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_0060_60/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_052_55/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delete_delta_052_55/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/base_50/bucket_0", 500, new byte[0]));
        Path part = new MockPath(fs, "mock:/tbl/part1");
        AcidUtils.Directory dir = AcidUtils.getAcidState(part, conf, new ValidReaderWriteIdList("tbl:100:" + Long.MAX_VALUE + ":"));
        assertThat(dir.getBaseDirectory().toString()).isEqualTo("mock:/tbl/part1/base_50");
        List<FileStatus> obsolete = dir.getObsolete();
        assertThat(obsolete).hasSize(3);
        assertThat(obsolete.get(0).getPath().toString()).isEqualTo("mock:/tbl/part1/delete_delta_052_55");
        assertThat(obsolete.get(1).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_052_55");
        assertThat(obsolete.get(2).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_0060_60");
        List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
        assertThat(delts).hasSize(6);
        assertThat(delts.get(0).getPath().toString()).isEqualTo("mock:/tbl/part1/delete_delta_40_60");
        assertThat(delts.get(1).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_40_60");
        assertThat(delts.get(2).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_00061_61");
        assertThat(delts.get(3).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_000062_62");
        assertThat(delts.get(4).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_0000063_63");
        assertThat(delts.get(5).getPath().toString()).isEqualTo("mock:/tbl/part1/delete_delta_00064_64");
    }

    @Test
    public void testMinorCompactedDeltaMakesInBetweenDelteDeltaObsolete()
            throws Exception
    {
        // This test checks that if we have a minor compacted delta for the txn range [40,60]
        // then it will make any delete delta in that range as obsolete.
        Configuration conf = new Configuration(false);
        MockFileSystem fs = new MockFileSystem(conf,
                new MockFile("mock:/tbl/part1/delta_40_60/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delete_delta_50_50/bucket_0", 500, new byte[0]));
        Path part = new MockPath(fs, "mock:/tbl/part1");
        AcidUtils.Directory dir = AcidUtils.getAcidState(part, conf, new ValidReaderWriteIdList("tbl:100:" + Long.MAX_VALUE + ":"));
        List<FileStatus> obsolete = dir.getObsolete();
        assertThat(obsolete).hasSize(1);
        assertThat(obsolete.get(0).getPath().toString()).isEqualTo("mock:/tbl/part1/delete_delta_50_50");
        List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
        assertThat(delts).hasSize(1);
        assertThat(delts.get(0).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_40_60");
    }

    @Test
    public void deltasAndDeleteDeltasWithOpenTxnsNotInCompact()
            throws Exception
    {
        // This tests checks that appropriate delta and delete_deltas are included when minor
        // compactions specifies a valid open txn range.
        Configuration conf = new Configuration(false);
        MockFileSystem fs = new MockFileSystem(conf,
                new MockFile("mock:/tbl/part1/delta_1_1/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delete_delta_2_2/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_2_5/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delete_delta_2_5/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_2_5/bucket_0" + AcidUtils.DELTA_SIDE_FILE_SUFFIX, 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delete_delta_7_7/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_6_10/bucket_0", 500, new byte[0]));
        Path part = new MockPath(fs, "mock:/tbl/part1");
        AcidUtils.Directory dir = AcidUtils.getAcidState(part, conf, new ValidCompactorWriteIdList("tbl:4:" + Long.MAX_VALUE + ":"));
        List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
        assertThat(delts).hasSize(2);
        assertThat(delts.get(0).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_1_1");
        assertThat(delts.get(1).getPath().toString()).isEqualTo("mock:/tbl/part1/delete_delta_2_2");
    }

    @Test
    public void deleteDeltasWithOpenTxnInRead()
            throws Exception
    {
        Configuration conf = new Configuration(false);
        MockFileSystem fs = new MockFileSystem(conf,
                new MockFile("mock:/tbl/part1/delta_1_1/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_2_5/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delete_delta_2_5/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delete_delta_3_3/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_4_4_1/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_4_4_3/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_101_101_1/bucket_0", 500, new byte[0]));
        Path part = new MockPath(fs, "mock:/tbl/part1");
        AcidUtils.Directory dir = AcidUtils.getAcidState(part, conf, new ValidReaderWriteIdList("tbl:100:4:4"));
        List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
        assertThat(delts).hasSize(3);
        assertThat(delts.get(0).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_1_1");
        assertThat(delts.get(1).getPath().toString()).isEqualTo("mock:/tbl/part1/delete_delta_2_5");
        assertThat(delts.get(2).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_2_5");
        // Note that delete_delta_3_3 should not be read, when a minor compacted
        // [delete_]delta_2_5 is present.
    }

    @Test
    public void testDeleteDeltaSubdirPathGeneration()
    {
        String deleteDeltaSubdirPath = AcidUtils.deleteDeltaSubdir(1, 10);
        assertThat(deleteDeltaSubdirPath).isEqualTo("delete_delta_0000001_0000010");
        deleteDeltaSubdirPath = AcidUtils.deleteDeltaSubdir(1, 10, 5);
        assertThat(deleteDeltaSubdirPath).isEqualTo("delete_delta_0000001_0000010_0005");
    }

    @Test
    public void testSkippingSubDirectories()
            throws IOException
    {
        Configuration conf = new Configuration(false);
        MockFileSystem fs = new MockFileSystem(conf,
                new MockFile("mock:/tbl/part1/base_1/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/base_1/base_1/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_025_025/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delta_025_025/delta_025_025/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delete_delta_029_029/bucket_0", 500, new byte[0]),
                new MockFile("mock:/tbl/part1/delete_delta_029_029/delete_delta_029_029/bucket_0", 500, new byte[0]));

        Path part = new MockPath(fs, "mock:/tbl/part1");
        AcidUtils.Directory dir = AcidUtils.getAcidState(part, conf, new ValidReaderWriteIdList("tbl:100:" + Long.MAX_VALUE + ":"));
        assertThat(dir.getBaseDirectory().toString()).isEqualTo("mock:/tbl/part1/base_1");
        List<FileStatus> obsolete = dir.getObsolete();
        assertThat(obsolete).isEmpty();
        List<AcidUtils.ParsedDelta> delts = dir.getCurrentDirectories();
        assertThat(delts).hasSize(2);
        assertThat(delts.get(0).getPath().toString()).isEqualTo("mock:/tbl/part1/delta_025_025");
        assertThat(delts.get(1).getPath().toString()).isEqualTo("mock:/tbl/part1/delete_delta_029_029");
    }
}
