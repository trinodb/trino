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
import io.prestosql.plugin.hive.PartitionStatistics;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.ConnectorIdentity;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestSemiTransactionalHiveMetastore
{
    @BeforeTest
    public void beforeTest()
    {
        FileSystemStats.clear();
        HiveMetastoreStats.clear();
    }

    @Test
    public void testCreateTable() throws Exception
    {
        SemiTransactionalHiveMetastore metastore = metastore();

        ConnectorSession session = session();

        Table table = table();
        PrincipalPrivileges principalPrivileges = mock(PrincipalPrivileges.class);

        PartitionStatistics statistics = mock(PartitionStatistics.class);
        metastore.createTable(session, table, principalPrivileges,
                Optional.of(new Path("hdfs://dir001/file")), false, statistics);

        assertEquals(
                metastore.tableActions.size(),
                1);

        assertEquals(
                SemiTransactionalHiveMetastore.ActionType.ADD,
                metastore.tableActions.get(new SchemaTableName("db001", "tbl001")).getType());
    }

    @Test
    public void testCreateTableAfterDropTableSuccess() throws Exception
    {
        SemiTransactionalHiveMetastore metastore = metastore();

        ConnectorSession session = session();
        metastore.dropTable(session, "db001", "tbl001");

        Table table = table();
        PrincipalPrivileges principalPrivileges = mock(PrincipalPrivileges.class);

        PartitionStatistics statistics = mock(PartitionStatistics.class);
        metastore.createTable(session, table, principalPrivileges,
                Optional.of(new Path("hdfs://dir001/file")), false, statistics);

        assertEquals(
                metastore.tableActions.size(),
                1);

        assertEquals(
                SemiTransactionalHiveMetastore.ActionType.ALTER,
                metastore.tableActions.get(new SchemaTableName("db001", "tbl001")).getType());
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testCreateTableAfterDropTableFailBecauseDiffUser() throws Exception
    {
        SemiTransactionalHiveMetastore metastore = metastore();

        ConnectorSession session = session("user002");
        metastore.dropTable(session, "db001", "tbl001");

        Table table = table();
        PrincipalPrivileges principalPrivileges = mock(PrincipalPrivileges.class);

        PartitionStatistics statistics = mock(PartitionStatistics.class);
        metastore.createTable(session, table, principalPrivileges,
                Optional.of(new Path("hdfs://dir001/file")), false, statistics);
    }

    @Test
    public void testCommitterPrepareAlterTable() throws Exception
    {
        SemiTransactionalHiveMetastore metastore = metastore();
        ConnectorSession session = session();
        metastore.dropTable(session, "db001", "tbl001");
        Table table = table("hdfs://dir001/tbl001/");
        PrincipalPrivileges principalPrivileges = mock(PrincipalPrivileges.class);

        PartitionStatistics statistics = mock(PartitionStatistics.class);
        metastore.createTable(session, table, principalPrivileges,
                Optional.of(new Path("hdfs://tmp/tbl001/")), false, statistics);

        metastore.commit();

        // 1. check the path renames
        assertEquals(
                FileSystemStats.renames,
                ImmutableMap.of(
                        // table location -> staging
                        "hdfs://dir001/tbl001", "hdfs://dir001/_temp_tbl001_query001",
                        // write path -> target path
                        "hdfs://tmp/tbl001", "hdfs://dir001/tbl001"));

        // 2. check the metastore operations
        assertEquals(
                HiveMetastoreStats.replaces,
                ImmutableSet.of(new SchemaTableName("db001", "tbl001")));
    }

    @Test
    public void testCommitterPrepareAlterTableRollback() throws Exception
    {
        SemiTransactionalHiveMetastore metastore = metastore(throwExceptionOnFirstInvokeHiveMetastore());
        ConnectorSession session = session();
        metastore.dropTable(session, "db001", "tbl001");
        Table table = table("hdfs://dir001/tbl001/");
        PrincipalPrivileges principalPrivileges = mock(PrincipalPrivileges.class);

        PartitionStatistics statistics = mock(PartitionStatistics.class);
        metastore.createTable(session, table, principalPrivileges,
                Optional.of(new Path("hdfs://tmp/tbl001/")), false, statistics);

        try {
            metastore.commit();
            fail();
        }
        catch (Throwable t) {
            // ignore
        }

        // 1. check the path renames
        assertEquals(
                FileSystemStats.renames,
                ImmutableMap.of(
                        // table location -> staging
                        "hdfs://dir001/tbl001", "hdfs://dir001/_temp_tbl001_query001",
                        // write path -> target path
                        "hdfs://tmp/tbl001", "hdfs://dir001/tbl001"));

        // 2. check the metastore operations
        assertEquals(
                HiveMetastoreStats.replaces,
                ImmutableList.of(
                        // first replaceTable
                        new SchemaTableName("db001", "tbl001"),
                        // rollback the replaceTable
                        new SchemaTableName("db001", "tbl001")));
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testCommitterPrepareAlterTableRollbackFailed() throws Exception
    {
        SemiTransactionalHiveMetastore metastore = metastore(throwExceptionOnEveryInvokeHiveMetastore());
        ConnectorSession session = session();
        metastore.dropTable(session, "db001", "tbl001");
        Table table = table("hdfs://dir001/tbl001/");
        PrincipalPrivileges principalPrivileges = mock(PrincipalPrivileges.class);

        PartitionStatistics statistics = mock(PartitionStatistics.class);
        metastore.createTable(session, table, principalPrivileges,
                Optional.of(new Path("hdfs://tmp/tbl001/")), false, statistics);

        metastore.commit();
    }

    private Table table()
    {
        return table("hdfs://dir001/tbl001/");
    }

    private Table table(String path)
    {
        Table table = mock(Table.class);
        doReturn("db001").when(table).getDatabaseName();
        doReturn("tbl001").when(table).getTableName();
        Storage storage = mock(Storage.class);
        doReturn(path).when(storage).getLocation();
        doReturn(storage).when(table).getStorage();
        return table;
    }

    private ConnectorSession session()
    {
        return session("user001");
    }

    private ConnectorSession session(String sessionUser)
    {
        ConnectorSession session = mock(ConnectorSession.class);
        ConnectorIdentity identity = mock(ConnectorIdentity.class);
        doReturn("user001").when(identity).getUser();
        doReturn(identity).when(session).getIdentity();
        doReturn("query001").when(session).getQueryId();
        doReturn(Optional.of("source001")).when(session).getSource();
        doReturn(sessionUser).when(session).getUser();
        return session;
    }

    private SemiTransactionalHiveMetastore metastore() throws Exception
    {
        return metastore(hiveMetastore());
    }

    private SemiTransactionalHiveMetastore metastore(HiveMetastore hiveMetastore) throws Exception
    {
        HdfsEnvironment hdfsEnvironment = hdfsEnvironment();
        HiveMetastore delegate = hiveMetastore;
        Executor renameExecutor = mock(Executor.class);
        boolean skipDeletionForAlter = false;
        boolean skipTargetCleanupOnRollback = false;

        return new SemiTransactionalHiveMetastore(
                hdfsEnvironment, delegate, renameExecutor, skipDeletionForAlter, skipTargetCleanupOnRollback);
    }

    private HiveMetastore hiveMetastore()
    {
        return hiveMetastore(true);
    }

    private HiveMetastore hiveMetastore(boolean getTableExists)
    {
        HiveMetastore delegate = mock(HiveMetastore.class);
        doAnswer(x -> {
            String dbName = x.getArgumentAt(0, String.class);
            String tableName = x.getArgumentAt(1, String.class);
            HiveMetastoreStats.replaceTable(dbName, tableName);
            return null;
        }).when(delegate).replaceTable(anyString(), anyString(), any(Table.class), any(PrincipalPrivileges.class));

        Optional<Table> table = getTableExists ? Optional.of(table()) : Optional.empty();
        doReturn(table).when(delegate).getTable(anyString(), anyString());

        return delegate;
    }

    private HiveMetastore throwExceptionOnFirstInvokeHiveMetastore()
    {
        HiveMetastore delegate = mock(HiveMetastore.class);
        doAnswer(x -> {
            boolean throwException = HiveMetastoreStats.replaces.isEmpty();
            String dbName = x.getArgumentAt(0, String.class);
            String tableName = x.getArgumentAt(1, String.class);
            HiveMetastoreStats.replaceTable(dbName, tableName);

            if (throwException) {
                throw new RuntimeException();
            }

            return null;
        }).when(delegate).replaceTable(anyString(), anyString(), any(Table.class), any(PrincipalPrivileges.class));
        doReturn(Optional.of(table())).when(delegate).getTable(anyString(), anyString());

        return delegate;
    }

    private HiveMetastore throwExceptionOnEveryInvokeHiveMetastore()
    {
        HiveMetastore delegate = mock(HiveMetastore.class);
        doAnswer(x -> {
            throw new RuntimeException();
        }).when(delegate).replaceTable(anyString(), anyString(), any(Table.class), any(PrincipalPrivileges.class));

        doReturn(Optional.of(table())).when(delegate).getTable(anyString(), anyString());
        return delegate;
    }

    private HdfsEnvironment hdfsEnvironment() throws Exception
    {
        return hdfsEnvironment(ImmutableList.of());
    }

    private HdfsEnvironment hdfsEnvironment(List<String> existsPathes) throws Exception
    {
        HdfsEnvironment hdfsEnvironment = mock(HdfsEnvironment.class);
        doReturn(fileSystem(existsPathes)).when(hdfsEnvironment).getFileSystem(any(), any());
        return hdfsEnvironment;
    }

    private FileSystem fileSystem(List<String> existsPathes) throws Exception
    {
        FileSystem fileSystem = mock(FileSystem.class);
        doAnswer(x -> {
            Path path = x.getArgumentAt(0, Path.class);
            return existsPathes.contains(path.toUri().toString());
        }).when(fileSystem).exists(any(Path.class));

        doAnswer(x -> {
            Path from = x.getArgumentAt(0, Path.class);
            Path to = x.getArgumentAt(1, Path.class);
            FileSystemStats.registerRenames(from.toUri().toString(), to.toUri().toString());
            return true;
        }).when(fileSystem).rename(any(Path.class), any(Path.class));

        doReturn(true).when(fileSystem).mkdirs(any(Path.class), any(FsPermission.class));
        doNothing().when(fileSystem).setPermission(any(Path.class), any(FsPermission.class));
        return fileSystem;
    }

    static class FileSystemStats
    {
        private static Map<String, String> renames = new HashMap<>();

        public static void registerRenames(String from, String to)
        {
            renames.put(from, to);
        }

        public static void clear()
        {
            renames.clear();
        }
    }

    static class HiveMetastoreStats
    {
        private static List<SchemaTableName> replaces = new ArrayList<>();

        public static void replaceTable(String dbname, String tableName)
        {
            replaces.add(new SchemaTableName(dbname, tableName));
        }

        public static void clear()
        {
            replaces.clear();
        }
    }
}
