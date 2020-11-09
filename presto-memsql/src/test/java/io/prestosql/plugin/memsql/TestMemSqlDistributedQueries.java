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
package io.prestosql.plugin.memsql;

import com.google.common.collect.ImmutableMap;
import io.prestosql.testing.AbstractTestDistributedQueries;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.sql.SqlExecutor;
import io.prestosql.testing.sql.TestTable;
import io.prestosql.tpch.TpchTable;
import org.testng.SkipException;

import java.util.Optional;

import static com.google.common.base.Strings.nullToEmpty;
import static io.prestosql.plugin.memsql.MemSqlQueryRunner.createMemSqlQueryRunner;

public class TestMemSqlDistributedQueries
        extends AbstractTestDistributedQueries
{
    protected TestingMemSqlServer memSqlServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        memSqlServer = new TestingMemSqlServer();
        closeAfterClass(() -> {
            memSqlServer.close();
            memSqlServer = null;
        });
        return createMemSqlQueryRunner(
                memSqlServer,
                ImmutableMap.<String, String>builder()
                        // caching here speeds up tests highly, caching is not used in smoke tests
                        .put("metadata.cache-ttl", "10m")
                        .put("metadata.cache-missing", "true")
                        .build(),
                TpchTable.getTables());
    }

    @Override
    protected boolean supportsDelete()
    {
        return false;
    }

    @Override
    protected boolean supportsViews()
    {
        return false;
    }

    @Override
    protected boolean supportsArrays()
    {
        return false;
    }

    @Override
    protected boolean supportsCommentOnTable()
    {
        return false;
    }

    @Override
    protected boolean supportsCommentOnColumn()
    {
        return false;
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                createJdbcSqlExecutor(),
                "tpch.table",
                "(col_required BIGINT NOT NULL," +
                        "col_nullable BIGINT," +
                        "col_default BIGINT DEFAULT 43," +
                        "col_nonnull_default BIGINT NOT NULL DEFAULT 42," +
                        "col_required2 BIGINT NOT NULL)");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getPrestoTypeName();
        if (typeName.equals("time")
                || typeName.equals("timestamp(3) with time zone")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        if (typeName.equals("real")
                || typeName.equals("timestamp")) {
            // TODO this should either work or fail cleanly
            return Optional.empty();
        }

        if (typeName.equals("varchar")) {
            // TODO fails due to case insensitive UTF-8 comparisons
            return Optional.empty();
        }

        return Optional.of(dataMappingTestSetup);
    }

    @Override
    protected boolean isColumnNameRejected(Exception exception, String columnName, boolean delimited)
    {
        return nullToEmpty(exception.getMessage()).matches(".*Incorrect column name.*");
    }

    @Override
    public void testLargeIn()
    {
        // Caused by: java.sql.SQLException: Thread stack overrun:  Used: 1048713 of a 1048576 stack. Use 'memsqld --thread_stack=#' to specify a bigger stack if needed.
        throw new SkipException("MemSQL's default thread stack is not large enough");
    }

    @Override
    public void testInsertUnicode()
    {
        // MemSQL's utf8 encoding is 3 bytes and truncates strings upon encountering a 4 byte sequence
        throw new SkipException("MemSQL doesn't support utf8mb4");
    }

    // MemSQL specific tests should normally go in TestMemSqlIntegrationSmokeTest

    protected SqlExecutor createJdbcSqlExecutor()
    {
        return memSqlServer::execute;
    }
}
