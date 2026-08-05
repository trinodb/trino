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
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.hdfs.HdfsTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.plugin.deltalake.DeltaTestingConnectorSession.SESSION;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.findLatestCommitVersion;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.getMandatoryCurrentVersion;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.readPartitionTimestampWithZone;
import static io.trino.plugin.deltalake.transactionlog.TransactionLogParser.readVersionChecksumFile;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTransactionLogParser
{
    @Test
    public void testGetCurrentVersion()
            throws Exception
    {
        TrinoFileSystem fileSystem = HDFS_FILE_SYSTEM_FACTORY.create(SESSION);

        String basePath = getClass().getClassLoader().getResource("databricks73").toURI().toString();

        assertThat(getMandatoryCurrentVersion(fileSystem, basePath + "/simple_table_without_checkpoint", 8)).isEqualTo(9);
        assertThat(getMandatoryCurrentVersion(fileSystem, basePath + "/simple_table_without_checkpoint", 9)).isEqualTo(9);
        assertThat(getMandatoryCurrentVersion(fileSystem, basePath + "/simple_table_ending_on_checkpoint", 10)).isEqualTo(10);
        assertThat(getMandatoryCurrentVersion(fileSystem, basePath + "/simple_table_past_checkpoint", 10)).isEqualTo(11);
        assertThat(getMandatoryCurrentVersion(fileSystem, basePath + "/simple_table_past_checkpoint", 11)).isEqualTo(11);
    }

    @Test
    void testReadPartitionTimestampWithZone()
    {
        assertThat(readPartitionTimestampWithZone("1970-01-01 00:00:00")).isEqualTo(0L);
        assertThat(readPartitionTimestampWithZone("1970-01-01 00:00:00.1")).isEqualTo(409600L);
        assertThat(readPartitionTimestampWithZone("1970-01-01 00:00:00.01")).isEqualTo(40960L);
        assertThat(readPartitionTimestampWithZone("1970-01-01 00:00:00.001")).isEqualTo(4096L);

        // https://github.com/trinodb/trino/issues/20359 Increase timestamp precision to microseconds
        assertThat(readPartitionTimestampWithZone("1970-01-01 00:00:00.0001")).isEqualTo(0L);
        assertThat(readPartitionTimestampWithZone("1970-01-01 00:00:00.00001")).isEqualTo(0L);
        assertThat(readPartitionTimestampWithZone("1970-01-01 00:00:00.000001")).isEqualTo(0L);
    }

    @Test
    void testReadPartitionTimestampWithZoneIso8601()
    {
        assertThat(readPartitionTimestampWithZone("1970-01-01T00:00:00.000000Z")).isEqualTo(0L);
        assertThat(readPartitionTimestampWithZone("1970-01-01T01:00:00.000000+01:00")).isEqualTo(0L);
    }

    /**
     * @see deltalake.checksum
     */
    @Test
    public void testFindLatestCommitVersion()
            throws Exception
    {
        TrinoFileSystem fileSystem = HDFS_FILE_SYSTEM_FACTORY.create(SESSION);
        String tableLocation = getClass().getClassLoader().getResource("deltalake/checksum").toURI().toString();

        assertThat(findLatestCommitVersion(fileSystem, tableLocation)).hasValue(1L);
    }

    /**
     * @see deltalake.checksum
     */
    @Test
    public void testReadVersionChecksum()
            throws Exception
    {
        TrinoFileSystem fileSystem = HDFS_FILE_SYSTEM_FACTORY.create(SESSION);
        String tableLocation = getClass().getClassLoader().getResource("deltalake/checksum").toURI().toString();

        DeltaLakeVersionChecksum checksum = readVersionChecksumFile(fileSystem, tableLocation, 1).orElseThrow();
        assertThat(checksum.metadata().orElseThrow().getId()).isEqualTo("a953d1d0-a84e-4ca6-bb2a-ed181213a3f0");
        assertThat(checksum.metadata().orElseThrow().getLowercasePartitionColumns()).isEmpty();
        assertThat(checksum.metadata().orElseThrow().getConfiguration())
                .containsEntry("delta.checkpointInterval", "1")
                .hasSize(1);
        assertThat(checksum.protocol()).hasValue(new ProtocolEntry(1, 2, Optional.empty(), Optional.empty()));
    }

    /**
     * @see deltalake.checksum_missing_latest
     */
    @Test
    public void testReadVersionChecksumMissingFile()
            throws Exception
    {
        TrinoFileSystem fileSystem = HDFS_FILE_SYSTEM_FACTORY.create(SESSION);
        String tableLocation = getClass().getClassLoader().getResource("deltalake/checksum_missing_latest").toURI().toString();

        assertThat(readVersionChecksumFile(fileSystem, tableLocation, 1)).isEmpty();
    }

    /**
     * @see deltalake.checksum_invalid_json
     */
    @Test
    public void testReadVersionChecksumInvalidJson()
            throws Exception
    {
        TrinoFileSystem fileSystem = HDFS_FILE_SYSTEM_FACTORY.create(SESSION);
        String tableLocation = getClass().getClassLoader().getResource("deltalake/checksum_invalid_json").toURI().toString();

        assertThat(readVersionChecksumFile(fileSystem, tableLocation, 1)).isEmpty();
    }

    /**
     * @see deltalake.checksum_invalid_json_mapping
     */
    @Test
    public void testReadVersionChecksumInvalidJsonMapping()
            throws Exception
    {
        TrinoFileSystem fileSystem = HDFS_FILE_SYSTEM_FACTORY.create(SESSION);
        String tableLocation = getClass().getClassLoader().getResource("deltalake/checksum_invalid_json_mapping").toURI().toString();

        assertThat(readVersionChecksumFile(fileSystem, tableLocation, 1)).isEmpty();
    }

    /**
     * @see deltalake.checksum_trailing_json_content
     */
    @Test
    public void testReadVersionChecksumJsonWithTrailingContent()
            throws Exception
    {
        TrinoFileSystem fileSystem = HDFS_FILE_SYSTEM_FACTORY.create(SESSION);
        String tableLocation = getClass().getClassLoader().getResource("deltalake/checksum_trailing_json_content").toURI().toString();

        assertThat(readVersionChecksumFile(fileSystem, tableLocation, 1)).isEmpty();
    }
}
