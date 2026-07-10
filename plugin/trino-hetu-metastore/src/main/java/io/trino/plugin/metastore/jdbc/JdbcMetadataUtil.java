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
package io.trino.plugin.metastore.jdbc;

import io.airlift.log.Logger;
import io.trino.spi.TrinoException;
import org.jdbi.v3.core.ConnectionException;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.HandleConsumer;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.JdbiException;

import java.lang.reflect.InvocationTargetException;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.reflect.Reflection.newProxy;
import static io.trino.spi.metastore.HetuErrorCode.HETU_METASTORE_CODE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * database util
 *
 * @since 2020-03-03
 */
public class JdbcMetadataUtil
{
    private static final Logger LOG = Logger.get(JdbcMetadataUtil.class);
    private static final int MAX_RETRY_COUNT = 3;
    private static final long MILLISECONDS_PER_SECOND = 1000L;
    private static final String ERROR_MESSAGE = "Trino metastore %s operation failed.";

    private JdbcMetadataUtil()
    {
    }

    /**
     * package jdbc on command
     *
     * @param jdbi    jdbi
     * @param daoType daoType
     * @param <T>     daotype
     * @return new proxy instance
     */
    public static <T> T onDemand(Jdbi jdbi, Class<T> daoType)
    {
        requireNonNull(jdbi, "jdbi is null");
        return newProxy(daoType, (proxy, method, args) -> {
            try {
                T dao = jdbi.onDemand(daoType);
                return method.invoke(dao, args);
            }
            catch (JdbiException e) {
                throw new TrinoException(HETU_METASTORE_CODE, format(ERROR_MESSAGE, method.getName()), e);
            }
            catch (InvocationTargetException e) {
                throw new TrinoException(HETU_METASTORE_CODE, format(ERROR_MESSAGE, method.getName()), e.getCause());
            }
        });
    }

    /**
     * create table with retry
     *
     * @param jdbi jdbi
     */
    public static void createTablesWithRetry(Jdbi jdbi)
    {
        final int delay = 1;
        int count = 1;
        do {
            try (Handle handle = jdbi.open()) {
                MetadataTableDao dao = handle.attach(MetadataTableDao.class);
                createAllMetadataTables(dao);
                return;
            }
            catch (ConnectionException e) {
                if (count == MAX_RETRY_COUNT) {
                    throw new TrinoException(HETU_METASTORE_CODE,
                            format("After reaching the maximum %s retries,the metadata table creation failed."
                                    + "Current only support mysql database, please check.", MAX_RETRY_COUNT), e);
                }
                LOG.warn("Failed to connect to database. Will retry again in %s seconds. Exception: %s",
                        delay * count, e);
                try {
                    Thread.sleep(convertSecondsToMilliseconds(delay * count));
                }
                catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
            }
            count++;
        } while (count != MAX_RETRY_COUNT + 1);
    }

    private static long convertSecondsToMilliseconds(int seconds)
    {
        return seconds * MILLISECONDS_PER_SECOND;
    }

    /**
     * run the transaction
     *
     * @param jdbi     jdbi
     * @param callback callback
     */
    public static void runTransaction(Jdbi jdbi, HandleConsumer<TrinoException> callback)
    {
        try {
            jdbi.useTransaction(callback);
        }
        catch (JdbiException e) {
            if (e.getCause() != null) {
                throwIfInstanceOf(e.getCause(), TrinoException.class);
            }
            throw new TrinoException(HETU_METASTORE_CODE, "Trino metastore operation failed.", e);
        }
    }

    /**
     * run the transaction with lock
     *
     * @param jdbi     jdbi
     * @param callback callback
     */
    public static void runTransactionWithLock(Jdbi jdbi, HandleConsumer<TrinoException> callback)
    {
        JdbcBasedLock jdbcLock = new JdbcBasedLock(jdbi);
        try {
            jdbcLock.lock();
            jdbi.useTransaction(callback);
        }
        catch (JdbiException e) {
            if (e.getCause() != null) {
                throwIfInstanceOf(e.getCause(), TrinoException.class);
            }
            throw new TrinoException(HETU_METASTORE_CODE, "Trino metastore operation failed.", e);
        }
        finally {
            jdbcLock.unlock();
        }
    }

    /**
     * create all metadata tables of hetu metastore
     *
     * @param tableDao table dao
     */
    public static void createAllMetadataTables(MetadataTableDao tableDao)
    {
        // hetu_ctlgs table
        tableDao.createTableCatalogs();
        // hetu_catalog_params
        tableDao.createTableCatalogParameters();
        // hetu_dbs table
        tableDao.createTableDatabases();
        // hetu_database_params table
        tableDao.createTableDatabaseParameters();
        // hetu_tbls table
        tableDao.createTableTables();
        // hetu_table_params table
        tableDao.createTableTableParameters();
        // hetu_tab_cols table
        tableDao.createTableColumns();
        // hetu_column_params table
        tableDao.createTableColumnParameters();
        // hetu_tab_lock table
        tableDao.createTableLock();
    }
}
