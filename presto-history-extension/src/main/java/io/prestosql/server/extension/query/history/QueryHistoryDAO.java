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
package io.prestosql.server.extension.query.history;

import org.jdbi.v3.core.transaction.TransactionIsolationLevel;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.transaction.Transaction;

public interface QueryHistoryDAO
{
    // DDL
    String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS query_history (" +
            "id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
            "cluster VARCHAR(20) NOT NULL, " +
            "query_id VARCHAR(100) UNIQUE NOT NULL, " +
            "query_state VARCHAR(10) NOT NULL, " +
            "user VARCHAR(50) NOT NULL, " +
            "source VARCHAR(50), " +
            "catalog VARCHAR(20), " +
            "create_time TIMESTAMP NOT NULL, " +
            "end_time TIMESTAMP, " +
            "query VARCHAR(2000) NOT NULL, " +
            "query_info LONGTEXT COMPRESSED=zlib NOT NULL, " +
            "INDEX idx_user (user), " +
            "INDEX idx_create_time (create_time), " +
            "INDEX idx_end_time (end_time))";

    // DDL for test purpose (the compressed attribute is not yet supported in current MariaDB4J version for test)
    String CREATE_TABLE_TEST = "CREATE TABLE IF NOT EXISTS query_history (" +
            "id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, " +
            "cluster VARCHAR(20) NOT NULL, " +
            "query_id VARCHAR(100) UNIQUE NOT NULL, " +
            "query_state VARCHAR(10) NOT NULL, " +
            "user VARCHAR(50) NOT NULL, " +
            "source VARCHAR(50), " +
            "catalog VARCHAR(20), " +
            "create_time TIMESTAMP NOT NULL, " +
            "end_time TIMESTAMP, " +
            "query VARCHAR(2000) NOT NULL, " +
            "query_info LONGTEXT NOT NULL, " +
            "INDEX idx_user (user), " +
            "INDEX idx_create_time (create_time), " +
            "INDEX idx_end_time (end_time))";

    /**
     *  For test only, you need to create the table in preprod/prod by using CREATE_TABLE statement before enable the extension.
     */
    @Transaction
    @SqlUpdate(CREATE_TABLE_TEST)
    void createQueryHistoryTable();

    @Transaction(TransactionIsolationLevel.READ_COMMITTED)
    @SqlUpdate("INSERT INTO query_history" +
            "(cluster, query_id, query_state, user, source, catalog, create_time, end_time, query, query_info) VALUES" +
            "(:cluster, :queryId, :queryState, :user, :source, :catalog, :createTime, :endTime, :query, :queryInfo)")
    void insertQueryHistory(@BindBean QueryHistory queryHistory);

    @SqlQuery("SELECT query_info FROM query_history WHERE query_id = :query_id")
    String getQueryInfoByQueryId(@Bind("query_id") String queryId);
}
