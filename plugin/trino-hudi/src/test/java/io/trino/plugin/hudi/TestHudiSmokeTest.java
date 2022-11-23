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
package io.trino.plugin.hudi;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hudi.testing.ResourceHudiTablesInitializer;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZonedDateTime;

import static io.trino.plugin.hudi.HudiQueryRunner.createHudiQueryRunner;
import static io.trino.plugin.hudi.testing.ResourceHudiTablesInitializer.TestingTable.HUDI_COW_PT_TBL;
import static io.trino.plugin.hudi.testing.ResourceHudiTablesInitializer.TestingTable.HUDI_NON_PART_COW;
import static io.trino.plugin.hudi.testing.ResourceHudiTablesInitializer.TestingTable.STOCK_TICKS_COW;
import static io.trino.plugin.hudi.testing.ResourceHudiTablesInitializer.TestingTable.STOCK_TICKS_MOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestHudiSmokeTest
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createHudiQueryRunner(ImmutableMap.of(), ImmutableMap.of(), new ResourceHudiTablesInitializer());
    }

    @Test
    public void testReadNonPartitionedTable()
    {
        assertQuery(
                "SELECT rowid, name FROM " + HUDI_NON_PART_COW,
                "SELECT * FROM VALUES ('row_1', 'bob'), ('row_2', 'john'), ('row_3', 'tom')");
    }

    @Test
    public void testReadPartitionedTables()
    {
        assertQuery("SELECT symbol, max(ts) FROM " + STOCK_TICKS_COW + " GROUP BY symbol HAVING symbol = 'GOOG'",
                "SELECT * FROM VALUES ('GOOG', '2018-08-31 10:59:00')");

        assertQuery("SELECT symbol, max(ts) FROM " + STOCK_TICKS_MOR + " GROUP BY symbol HAVING symbol = 'GOOG'",
                "SELECT * FROM VALUES ('GOOG', '2018-08-31 10:59:00')");

        assertQuery("SELECT dt, count(1) FROM " + STOCK_TICKS_MOR + " GROUP BY dt",
                "SELECT * FROM VALUES ('2018-08-31', '99')");
    }

    @Test
    public void testMultiPartitionedTable()
    {
        assertQuery("SELECT _hoodie_partition_path, id, name, ts, dt, hh FROM " + HUDI_COW_PT_TBL + " WHERE id = 1",
                "SELECT * FROM VALUES ('dt=2021-12-09/hh=10', 1, 'a1', 1000, '2021-12-09', '10')");
        assertQuery("SELECT _hoodie_partition_path, id, name, ts, dt, hh FROM " + HUDI_COW_PT_TBL + " WHERE id = 2",
                "SELECT * FROM VALUES ('dt=2021-12-09/hh=11', 2, 'a2', 1000, '2021-12-09', '11')");
    }

    @Test
    public void testShowCreateTable()
    {
        assertThat((String) computeActual("SHOW CREATE TABLE " + STOCK_TICKS_COW).getOnlyValue())
                .matches("CREATE TABLE \\w+\\.\\w+\\.stock_ticks_cow \\Q(\n" +
                        "   _hoodie_commit_time varchar,\n" +
                        "   _hoodie_commit_seqno varchar,\n" +
                        "   _hoodie_record_key varchar,\n" +
                        "   _hoodie_partition_path varchar,\n" +
                        "   _hoodie_file_name varchar,\n" +
                        "   volume bigint,\n" +
                        "   ts varchar,\n" +
                        "   symbol varchar,\n" +
                        "   year integer,\n" +
                        "   month varchar,\n" +
                        "   high double,\n" +
                        "   low double,\n" +
                        "   key varchar,\n" +
                        "   date varchar,\n" +
                        "   close double,\n" +
                        "   open double,\n" +
                        "   day varchar,\n" +
                        "   dt varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   location = \\E'.*/stock_ticks_cow',\n\\Q" +
                        "   partitioned_by = ARRAY['dt']\n" +
                        ")");
        // multi-partitioned table
        assertThat((String) computeActual("SHOW CREATE TABLE " + HUDI_COW_PT_TBL).getOnlyValue())
                .matches("CREATE TABLE \\w+\\.\\w+\\.hudi_cow_pt_tbl \\Q(\n" +
                        "   _hoodie_commit_time varchar,\n" +
                        "   _hoodie_commit_seqno varchar,\n" +
                        "   _hoodie_record_key varchar,\n" +
                        "   _hoodie_partition_path varchar,\n" +
                        "   _hoodie_file_name varchar,\n" +
                        "   id bigint,\n" +
                        "   name varchar,\n" +
                        "   ts bigint,\n" +
                        "   dt varchar,\n" +
                        "   hh varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   location = \\E'.*/hudi_cow_pt_tbl',\n\\Q" +
                        "   partitioned_by = ARRAY['dt','hh']\n" +
                        ")");
    }

    @Test
    public void testMetaColumns()
    {
        assertQuery("SELECT _hoodie_commit_time FROM hudi_cow_pt_tbl", "VALUES ('20220906063435640'), ('20220906063456550')");
        assertQuery("SELECT _hoodie_commit_seqno FROM hudi_cow_pt_tbl", "VALUES ('20220906063435640_0_0'), ('20220906063456550_0_0')");
        assertQuery("SELECT _hoodie_record_key FROM hudi_cow_pt_tbl", "VALUES ('id:1'), ('id:2')");
        assertQuery("SELECT _hoodie_partition_path FROM hudi_cow_pt_tbl", "VALUES ('dt=2021-12-09/hh=10'), ('dt=2021-12-09/hh=11')");
        assertQuery(
                "SELECT _hoodie_file_name FROM hudi_cow_pt_tbl",
                "VALUES ('719c3273-2805-4124-b1ac-e980dada85bf-0_0-27-1215_20220906063435640.parquet'), ('4a3fcb9b-65eb-4f6e-acf9-7b0764bb4dd1-0_0-70-2444_20220906063456550.parquet')");
    }

    @Test
    public void testPathColumn()
    {
        String path = (String) computeScalar("SELECT \"$path\" FROM " + HUDI_COW_PT_TBL + " WHERE id = 1");
        assertThat(toPath(path)).exists();
    }

    @Test
    public void testFileSizeColumn()
            throws Exception
    {
        String path = (String) computeScalar("SELECT \"$path\" FROM " + HUDI_COW_PT_TBL + " WHERE id = 1");
        long fileSize = (long) computeScalar("SELECT \"$file_size\" FROM " + HUDI_COW_PT_TBL + " WHERE id = 1");
        assertEquals(fileSize, Files.size(toPath(path)));
    }

    @Test
    public void testFileModifiedColumn()
            throws Exception
    {
        String path = (String) computeScalar("SELECT \"$path\" FROM " + HUDI_COW_PT_TBL + " WHERE id = 1");
        ZonedDateTime fileModifiedTime = (ZonedDateTime) computeScalar("SELECT \"$file_modified_time\" FROM " + HUDI_COW_PT_TBL + " WHERE id = 1");
        assertEquals(fileModifiedTime.toInstant().toEpochMilli(), Files.getLastModifiedTime(toPath(path)).toInstant().toEpochMilli());
    }

    @Test
    public void testPartitionColumn()
    {
        assertQuery("SELECT \"$partition\" FROM " + HUDI_COW_PT_TBL + " WHERE id = 1", "VALUES 'dt=2021-12-09/hh=10'");
        assertQuery("SELECT \"$partition\" FROM " + HUDI_COW_PT_TBL + " WHERE id = 2", "VALUES 'dt=2021-12-09/hh=11'");

        assertQueryFails("SELECT \"$partition\" FROM " + HUDI_NON_PART_COW, ".* Column '\\$partition' cannot be resolved");
    }

    private static Path toPath(String path)
    {
        // Remove leading 'file:' because $path column returns 'file:/path-to-file' in case of local file system
        return Path.of(path.replaceFirst("^file:", ""));
    }
}
