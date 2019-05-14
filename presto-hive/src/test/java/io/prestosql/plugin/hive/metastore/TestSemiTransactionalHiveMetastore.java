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
package io.prestosql.plugin.hive.metastore;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveBasicStatistics;
import io.prestosql.plugin.hive.PartitionStatistics;
import io.prestosql.plugin.hive.TestBackgroundHiveSplitLoader;
import io.prestosql.plugin.hive.metastore.file.FileHiveMetastore;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.testing.TestingConnectorSession;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import java.util.Date;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.Executors;

import static org.testng.Assert.assertEquals;

public class TestSemiTransactionalHiveMetastore
{
    @Test
    public void testCreateTable()
    {
        SemiTransactionalHiveMetastore metastore = getSemiTransactionalHiveMetastore();
        metastore.createTable(
                session(),
                table(),
                principalPrivileges(),
                Optional.of(new Path("hdfs://dir001/file")),
                false,
                partitionStatistics());

        assertEquals(
                metastore.tableActions.size(),
                1);

        assertEquals(
                SemiTransactionalHiveMetastore.ActionType.ADD,
                metastore.tableActions.get(new SchemaTableName("db001", "tbl001")).getType());
    }

    @Test
    public void testCreateTableAfterDropTableSuccess()
    {
        SemiTransactionalHiveMetastore metastore = getSemiTransactionalHiveMetastore();

        metastore.dropTable(session(), "db001", "tbl001");
        metastore.createTable(
                session(),
                table(),
                principalPrivileges(),
                Optional.of(new Path("hdfs://dir001/file")),
                false,
                partitionStatistics());

        assertEquals(
                metastore.tableActions.size(),
                1);

        assertEquals(
                SemiTransactionalHiveMetastore.ActionType.ALTER,
                metastore.tableActions.get(new SchemaTableName("db001", "tbl001")).getType());
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testCreateTableAfterDropTableFailBecauseDiffUser()
    {
        SemiTransactionalHiveMetastore metastore = getSemiTransactionalHiveMetastore();
        metastore.dropTable(session("user001"), "db001", "tbl001");
        metastore.createTable(
                session("user002"),
                table(),
                principalPrivileges(),
                Optional.of(new Path("hdfs://dir001/file")),
                false,
                partitionStatistics());
    }

    private Table table()
    {
        return table("hdfs://dir001/tbl001/");
    }

    private Table table(String path)
    {
        Storage storage = new Storage(StorageFormat.VIEW_STORAGE_FORMAT, path, Optional.empty(), false, ImmutableMap.of());
        return new Table("db001", "tbl001", "user001", "BASE TABLE", storage, ImmutableList.of(), ImmutableList.of(), ImmutableMap.of(), Optional.empty(), Optional.empty());
    }

    private ConnectorSession session()
    {
        return session("user001");
    }

    private ConnectorSession session(String sessionUser)
    {
        return new TestingConnectorSession(
                sessionUser,
                Optional.of("source001"),
                Optional.of("trace001"),
                TimeZoneKey.UTC_KEY,
                Locale.getDefault(),
                new Date().getTime(),
                ImmutableList.of(),
                ImmutableMap.of(),
                false);
    }

    private PrincipalPrivileges principalPrivileges()
    {
        return PrincipalPrivileges.fromHivePrivilegeInfos(ImmutableSet.of());
    }

    private PartitionStatistics partitionStatistics()
    {
        return new PartitionStatistics(HiveBasicStatistics.createEmptyStatistics(), ImmutableMap.of());
    }

    private SemiTransactionalHiveMetastore getSemiTransactionalHiveMetastore()
    {
        HdfsEnvironment hdfsEnvironment = new TestBackgroundHiveSplitLoader.TestingHdfsEnvironment(ImmutableList.of());
        FileHiveMetastore hiveMetastore = new FileHiveMetastore(hdfsEnvironment, "/tmp/test", "user001");

        return new SemiTransactionalHiveMetastore(hdfsEnvironment, hiveMetastore, Executors.newSingleThreadExecutor(), false, false);
    }
}
