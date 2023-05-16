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
package io.trino.plugin.deltalake;

import io.trino.Session;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Verify.verify;
import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaLakeConcurrentWrites
        extends AbstractTestQueryFramework
{
    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("delta_lake")
                .setSchema("default")
                .build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .build();
        try {
            String metastoreDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("delta_lake_metastore").toFile().getAbsoluteFile().toURI().toString();
            String writeMetastoreDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("delta_lake_write_metastore").toFile().getAbsoluteFile().toURI().toString();

            queryRunner.installPlugin(new TestingDeltaLakePlugin(Optional.empty(), Optional.of(new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS)), EMPTY_MODULE));
            queryRunner.createCatalog(
                    "delta_lake",
                    "delta_lake",
                    Map.of(
                            "hive.metastore", "file",
                            "hive.metastore.catalog.dir", metastoreDirectory,
                            "delta.enable-non-concurrent-writes", "true"));
            queryRunner.createCatalog(
                    "delta_lake_writer",
                    "delta_lake",
                    Map.of(
                            "hive.metastore", "file",
                            "hive.metastore.catalog.dir", writeMetastoreDirectory,
                            "delta.register-table-procedure.enabled", "true",
                            "delta.default-checkpoint-writing-interval", "2",
                            "delta.enable-non-concurrent-writes", "true"));

            queryRunner.execute("CREATE SCHEMA " + session.getSchema().orElseThrow());
            queryRunner.execute("CREATE SCHEMA delta_lake_writer." + session.getSchema().orElseThrow());
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    @Test
    public void testMetadataEntryIsFresh()
    {
        String tableName = "test_checkpoint_metadata_file_operations";
        String schema = getSession().getSchema().orElseThrow();
        String writeTable = "delta_lake_writer." + schema + "." + tableName;

        assertUpdate("DROP TABLE IF EXISTS " + tableName);
        assertUpdate("CREATE TABLE " + tableName + "(c varchar) COMMENT 'commit-0' WITH (checkpoint_interval = 2)");
        assertUpdate("CALL delta_lake_writer.system.register_table(schema_name => '" + schema + "', table_name => '" + tableName + "', table_location => '" + getTableLocation(tableName) + "')");

        assertUpdate("COMMENT ON TABLE " + writeTable + " IS 'commit-1'");
        assertUpdate("INSERT INTO " + writeTable + " VALUES 'commit-2'", 1); // metadata commits do not trigger checkpoint writes
        assertThat(getTableComment(tableName)).isEqualTo("commit-1");

        assertUpdate("COMMENT ON TABLE " + writeTable + " IS 'commit-3'");
        assertUpdate("INSERT INTO " + writeTable + " VALUES 'commit-4'", 1);
        assertThat(getTableComment(tableName)).isEqualTo("commit-3");
    }

    private String getTableLocation(String tableName)
    {
        Pattern locationPattern = Pattern.compile(".*location = '(.*?)'.*", Pattern.DOTALL);
        Matcher m = locationPattern.matcher((String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue());
        if (m.find()) {
            String location = m.group(1);
            verify(!m.find(), "Unexpected second match");
            return location;
        }
        throw new IllegalStateException("Location not found in SHOW CREATE TABLE result");
    }

    private String getTableComment(String tableName)
    {
        Pattern commentPattern = Pattern.compile(".*COMMENT\\s+'([^']*)'.*", Pattern.DOTALL);
        Matcher m = commentPattern.matcher((String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue());
        if (m.find()) {
            String location = m.group(1);
            verify(!m.find(), "Unexpected second match");
            return location;
        }
        throw new IllegalStateException("COMMENT not found in SHOW CREATE TABLE result");
    }
}
