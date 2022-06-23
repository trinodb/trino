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
import io.trino.plugin.hive.SchemaAlreadyExistsException;
import io.trino.plugin.hive.metastore.Database;
import io.trino.spi.connector.SchemaNotFoundException;
import org.apache.thrift.TException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestAlibabaDlfDatabaseCache
{
    private static final String catalogId = "";
    private static final String testDB = TestingAlibabaDlfUtils.TEST_DB;
    private static boolean dbCacheEnable;
    private static CacheDataLakeMetaStore cacheDataLakeMetaStore;
    private static int dbCacheSize = 100;
    private static int dbCacheTTL = 2;
    private static int dbCacheSizeConf;
    private static int dbCacheTTLConf;
    private static IDataLakeMetaStore dataLakeMetaStore;
    private static ProxyTrinoClient client;
    private static AlibabaDlfMetaStoreConfig alibabaDlfMetaStoreConfig;

    @BeforeClass
    public static void setUp() throws TException, IOException
    {
        alibabaDlfMetaStoreConfig = new AlibabaDlfMetaStoreConfig();
        client = (ProxyTrinoClient) TestingAlibabaDlfUtils.getDlfClient(alibabaDlfMetaStoreConfig);

        if (alibabaDlfMetaStoreConfig.getDbCacheEnable().isPresent()) {
            dbCacheEnable = alibabaDlfMetaStoreConfig.getDbCacheEnable().get();
        }

        dataLakeMetaStore = ((AlibabaDlfMetaStoreClient) client.getDlfMetaStoreClient()).getDataLakeMetaStore();

        if (alibabaDlfMetaStoreConfig.getDbCacheSize().isPresent()) {
            dbCacheSizeConf = alibabaDlfMetaStoreConfig.getDbCacheSize().get();
        }

        if (alibabaDlfMetaStoreConfig.getDbCacheTTLMins().isPresent()) {
            dbCacheTTLConf = alibabaDlfMetaStoreConfig.getDbCacheTTLMins().get();
        }
    }

    @AfterClass
    public void cleanUp() throws TException
    {
        try {
            client.dropDatabase(testDB, true);
        }
        catch (SchemaNotFoundException e) {
        }
    }

    @Test
    public void testCacheEnable()
    {
        if (dbCacheEnable) {
            assertTrue(dataLakeMetaStore instanceof CacheDataLakeMetaStore);
            cacheDataLakeMetaStore = (CacheDataLakeMetaStore) dataLakeMetaStore;
            assertEquals(dbCacheSize, dbCacheSizeConf);
            assertEquals(dbCacheTTL, dbCacheTTLConf);
            assertTrue(cacheDataLakeMetaStore.getDatabaseCache() != null);
        }
    }

    @Test
    public void testCreateDatabaseCache() throws Exception
    {
        if (dbCacheEnable) {
            String databaseName = testDB;
            String location = TestingAlibabaDlfUtils.getDefaultDatabasePath(databaseName).toString();
            Database database0 = TestingAlibabaDlfUtils.getDatabase(databaseName, location);
            try {
                client.createDatabase(database0);
            }
            catch (SchemaAlreadyExistsException e) {
            }

            if (dataLakeMetaStore instanceof CacheDataLakeMetaStore) {
                cacheDataLakeMetaStore = (CacheDataLakeMetaStore) dataLakeMetaStore;
            }
            Optional<Database> database = client.getDatabase(databaseName);

            assertTrue(cacheDataLakeMetaStore.getDatabaseCache().size() >= 1);

            CacheDataLakeMetaStore.DbIdentifier dbIdentifier = new CacheDataLakeMetaStore.DbIdentifier(catalogId, databaseName);
            assertEquals(database.get().getDatabaseName(), cacheDataLakeMetaStore.getDatabaseCache().getIfPresent(dbIdentifier).getName());
            assertEquals(database.get().getLocation().get(), cacheDataLakeMetaStore.getDatabaseCache().getIfPresent(dbIdentifier).getLocationUri());

            //hit
            client.getDatabase(databaseName);

            //drop
            client.dropDatabase(databaseName, true);

            //invalidate
            assertTrue(cacheDataLakeMetaStore.getDatabaseCache().getIfPresent(dbIdentifier) == null, database.get().getDatabaseName());
        }
    }

    @Test
    public void testCreateDatabaseCacheTTL() throws Exception
    {
        if (dbCacheEnable) {
            String name = testDB;
            String location = TestingAlibabaDlfUtils.getDefaultDatabasePath(name).toString();
            Database database0 = TestingAlibabaDlfUtils.getDatabase(name, location);

            client.createDatabase(database0);

            if (dataLakeMetaStore instanceof CacheDataLakeMetaStore) {
                cacheDataLakeMetaStore = (CacheDataLakeMetaStore) dataLakeMetaStore;
            }
            Optional<Database> database = client.getDatabase(name);
            //控制台输出not hit；then init
            assertTrue(cacheDataLakeMetaStore.getDatabaseCache().size() >= 1);

            CacheDataLakeMetaStore.DbIdentifier dbIdentifier = new CacheDataLakeMetaStore.DbIdentifier(catalogId, name);
            assertEquals(database.get().getDatabaseName(), cacheDataLakeMetaStore.getDatabaseCache().getIfPresent(dbIdentifier).getName());
            assertEquals(database.get().getLocation().get(), cacheDataLakeMetaStore.getDatabaseCache().getIfPresent(dbIdentifier).getLocationUri());

            //hit
            client.getDatabase(name);

            //sleep 1/2 TTL ,cache still validate
            Thread.sleep((dbCacheTTLConf / 2) * 60 * 1000);
            assertTrue(cacheDataLakeMetaStore.getDatabaseCache().getIfPresent(dbIdentifier) != null, "now sleeped 1/2TTL,cache now is valided");
            client.getDatabase(name);

            // sleep 1 ttl，cache now invalidate
            Thread.sleep((dbCacheTTLConf) * 60 * 1000);
            assertTrue(cacheDataLakeMetaStore.getDatabaseCache().getIfPresent(dbIdentifier) == null, "now sleeped 1TTL,cache now is invalided");
            client.getDatabase(name);
            //database drop
            client.dropDatabase(name, true);

            //cache invalidate
            assertTrue(cacheDataLakeMetaStore.getDatabaseCache().getIfPresent(dbIdentifier) == null, "now sleeped 1TTL,cache now is invalided");
        }
    }

    @Test
    public void testCreateDatabaseCacheSize() throws Exception
    {
        if (dbCacheEnable) {
            String name = testDB;
            String location = TestingAlibabaDlfUtils.getDefaultDatabasePath(name).toString();
            TestingAlibabaDlfUtils.getDatabase(name, location);
            if (dataLakeMetaStore instanceof CacheDataLakeMetaStore) {
                cacheDataLakeMetaStore = (CacheDataLakeMetaStore) dataLakeMetaStore;
            }

            for (int i = 0; i < 100; i++) {
                try {
                    client.dropDatabase(name + i, true);
                }
                catch (SchemaNotFoundException e) {
                }
            }

            for (int i = 0; i < 100; i++) {
                client.createDatabase(TestingAlibabaDlfUtils.getDatabase(name + i, TestingAlibabaDlfUtils.getDefaultDatabasePath(name + i).toString()));
                client.getDatabase(name + i);
            }
            assertTrue(cacheDataLakeMetaStore.getDatabaseCache().size() <= Math.min(dbCacheSize, 100));
            for (int i = 0; i < 100; i++) {
                client.dropDatabase(name + i, true);
            }
            assertTrue(cacheDataLakeMetaStore.getDatabaseCache().size() <= 1);
        }
    }
}
