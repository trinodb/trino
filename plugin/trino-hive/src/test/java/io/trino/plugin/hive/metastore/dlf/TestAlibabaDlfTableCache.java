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
package io.trino.plugin.hive.metastore.dlf;

import com.aliyun.datalake.metastore.common.CacheDataLakeMetaStore;
import com.aliyun.datalake.metastore.common.IDataLakeMetaStore;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.connector.SchemaNotFoundException;
import org.apache.thrift.TException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestAlibabaDlfTableCache
{
    private static final String testDB = TestingAlibabaDlfUtils.TEST_DB;
    private static final String catalogId = "";
    private static boolean tbCacheEnable;
    private static CacheDataLakeMetaStore cacheDataLakeMetaStore;
    private static int tbCacheSize = 150;
    private static int tbCacheTTL = 1;
    private static int tbCacheSizeConf;
    private static int tbCacheTTLConf;
    private static IDataLakeMetaStore dataLakeMetaStore;
    private static ProxyTrinoClient client;
    private static AlibabaDlfMetaStoreConfig alibabaDlfMetaStoreConfig;
    private static Database testDb;

    @BeforeClass
    public static void setUp() throws TException, IOException
    {
        alibabaDlfMetaStoreConfig = new AlibabaDlfMetaStoreConfig();
        client = (ProxyTrinoClient) TestingAlibabaDlfUtils.getDlfClient(alibabaDlfMetaStoreConfig);
        String location = TestingAlibabaDlfUtils.getDefaultDatabasePath(testDB).toString();
        testDb = TestingAlibabaDlfUtils.getDatabase(testDB, location);
        try {
            client.createDatabase(testDb);
        }
        catch (Exception e) {
        }

        dataLakeMetaStore = ((AlibabaDlfMetaStoreClient) client.getDlfMetaStoreClient()).getDataLakeMetaStore();

        if (alibabaDlfMetaStoreConfig.getTbCacheEnable().isPresent()) {
            tbCacheEnable = alibabaDlfMetaStoreConfig.getTbCacheEnable().get();
        }

        if (alibabaDlfMetaStoreConfig.getTbCacheSize().isPresent()) {
            tbCacheSizeConf = alibabaDlfMetaStoreConfig.getTbCacheSize().get();
        }

        if (alibabaDlfMetaStoreConfig.getTbCacheTTLMins().isPresent()) {
            tbCacheTTLConf = alibabaDlfMetaStoreConfig.getTbCacheTTLMins().get();
        }
    }

    @AfterMethod
    public static void cleanUpCase() throws TException, IOException
    {
        List<String> allTables = client.getAllTables(testDB);
        allTables.forEach(t -> {
            try {
                client.dropTable(testDB, t, true);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @AfterClass
    public static void cleanUp() throws TException, IOException
    {
        try {
            client.dropDatabase(testDB, true);
        }
        catch (SchemaNotFoundException e) {
        }
    }

    @Test
    public void testTbCacheEnable()
    {
        if (tbCacheEnable) {
            assertTrue(dataLakeMetaStore instanceof CacheDataLakeMetaStore);
            cacheDataLakeMetaStore = (CacheDataLakeMetaStore) dataLakeMetaStore;
            assertEquals(tbCacheSize, tbCacheSizeConf);
            assertEquals(tbCacheTTL, tbCacheTTLConf);
            assertTrue(cacheDataLakeMetaStore.getTableCache() != null);
        }
    }

    @Test
    public void testCreateTableCache() throws Exception
    {
        if (tbCacheEnable) {
            String tblName = TestingAlibabaDlfUtils.TEST_TABLE;
            createDefaultTable(tblName);

            if (dataLakeMetaStore instanceof CacheDataLakeMetaStore) {
                cacheDataLakeMetaStore = (CacheDataLakeMetaStore) dataLakeMetaStore;
            }
            client.getTable(testDB, tblName);
            assertTrue(cacheDataLakeMetaStore.getTableCache().size() >= 1);

            CacheDataLakeMetaStore.TableIdentifier tbIdentifier = new CacheDataLakeMetaStore.TableIdentifier(catalogId, testDB, tblName);
            assertEquals(tbIdentifier.getTableName(), cacheDataLakeMetaStore.getTableCache().getIfPresent(tbIdentifier).getTableName());
            assertEquals(tbIdentifier.getDbName(), cacheDataLakeMetaStore.getTableCache().getIfPresent(tbIdentifier).getDatabaseName());

            //hit
            client.getTable(testDB, tblName);

            //drop
            client.dropTable(testDB, tblName, true);

            //invalidate
            assertTrue(cacheDataLakeMetaStore.getTableCache().getIfPresent(tbIdentifier) == null);
        }
    }

    @Test
    public void testCreateTableCacheTTL() throws Exception
    {
        if (tbCacheEnable) {
            String tblName = TestingAlibabaDlfUtils.TEST_TABLE;
            createDefaultTable(tblName);
            if (dataLakeMetaStore instanceof CacheDataLakeMetaStore) {
                cacheDataLakeMetaStore = (CacheDataLakeMetaStore) dataLakeMetaStore;
            }
            Optional<Table> table = client.getTable(testDB, tblName);
            assertTrue(cacheDataLakeMetaStore.getTableCache().size() >= 1);

            CacheDataLakeMetaStore.TableIdentifier tbIdentifier = new CacheDataLakeMetaStore.TableIdentifier(catalogId, testDB, tblName);
            assertEquals(table.get().getTableName(), cacheDataLakeMetaStore.getTableCache().getIfPresent(tbIdentifier).getTableName());
            assertEquals(table.get().getDatabaseName(), cacheDataLakeMetaStore.getTableCache().getIfPresent(tbIdentifier).getDatabaseName());

            //hit
            client.getTable(testDB, tblName);

            //sleep 1/2 TTL ,cache still validate
            Thread.sleep((tbCacheTTLConf / 2) * 60 * 1000);
            assertTrue(cacheDataLakeMetaStore.getTableCache().getIfPresent(tbIdentifier) != null, "now sleeped 1/2TTL,cache now is valided");
            client.getTable(testDB, tblName);

            // sleep 1 ttl，cache now invalidate
            Thread.sleep((tbCacheTTLConf) * 60 * 1000);
            assertTrue(cacheDataLakeMetaStore.getTableCache().getIfPresent(tbIdentifier) == null, "now sleeped 1TTL,cache now is invalided");
            client.getTable(testDB, tblName);
            //Table drop
            client.dropTable(testDB, tblName, true);

            //cache invalidate
            assertTrue(cacheDataLakeMetaStore.getTableCache().getIfPresent(tbIdentifier) == null, "now sleeped 1TTL,cache now is invalided");
        }
    }

    @Test
    public void testCreateTableCacheSize() throws Exception
    {
        if (tbCacheEnable) {
            String tblName = TestingAlibabaDlfUtils.TEST_TABLE;
            createDefaultTable(tblName);

            if (dataLakeMetaStore instanceof CacheDataLakeMetaStore) {
                cacheDataLakeMetaStore = (CacheDataLakeMetaStore) dataLakeMetaStore;
            }

            for (int i = 0; i < 100; i++) {
                createDefaultTable(tblName + i);
                client.getTable(testDB, tblName + i);
            }
            assertTrue(cacheDataLakeMetaStore.getTableCache().size() <= Math.min(tbCacheSize, 100));
            for (int i = 0; i < 100; i++) {
                client.dropTable(testDB, tblName + i, true);
            }
            assertTrue(cacheDataLakeMetaStore.getTableCache().size() == 0);
        }
    }

    @Test
    public void testAlterTableWithCache() throws Exception
    {
        String tblName = TestingAlibabaDlfUtils.TEST_TABLE;
        createDefaultTable(tblName);
        if (dataLakeMetaStore instanceof CacheDataLakeMetaStore) {
            cacheDataLakeMetaStore = (CacheDataLakeMetaStore) dataLakeMetaStore;
        }

        //now cache has no db
        CacheDataLakeMetaStore.TableIdentifier tbIdentifier = new CacheDataLakeMetaStore.TableIdentifier(catalogId, testDB, tblName);
        assertTrue(cacheDataLakeMetaStore.getTableCache().getIfPresent(tbIdentifier) == null);

        //not hit/init
        client.getTable(testDB, tblName);
        assertTrue(cacheDataLakeMetaStore.getTableCache().getIfPresent(tbIdentifier) != null);

        //alter then invalid
        //new table(name, "new table", "file:/tmp/new_db", null)
        String newTblName = TestingAlibabaDlfUtils.TEST_TABLE + "new";
        client.renameTable(testDB, tblName, testDB, newTblName);
        assertTrue(cacheDataLakeMetaStore.getTableCache().getIfPresent(tbIdentifier) == null);

        //drop
        client.dropTable(testDB, newTblName, true);
    }

    private Table getDefaultTable(String tblName)
    {
        Map<String, String> columns = ImmutableMap.of("id", "int", "name", "string");
        Map<String, String> partitionColumns = ImmutableMap.of();
        return TestingAlibabaDlfUtils.getTable(testDB, tblName, columns, partitionColumns);
    }

    private void createDefaultTable(String tblName)
    {
        // create a table
        client.createTable(getDefaultTable(tblName), null);
    }
}
