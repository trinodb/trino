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
package io.trino.tests.product.utils;

import io.trino.tempto.query.QueryExecutionException;
import io.trino.tempto.query.QueryExecutor;
import io.trino.tempto.query.QueryResult;
import net.jodah.failsafe.Failsafe;

import java.sql.Connection;

import static io.trino.tempto.context.ThreadLocalTestContextHolder.testContext;
import static io.trino.tests.product.hive.HiveProductTest.ERROR_COMMITTING_WRITE_TO_HIVE_RETRY_POLICY;

public final class QueryExecutors
{
    @Deprecated
    public static QueryExecutor onPresto()
    {
        return onTrino();
    }

    public static QueryExecutor onTrino()
    {
        return connectToPresto("presto");
    }

    public static QueryExecutor onCompatibilityTestServer()
    {
        return connectToPresto("compatibility-test-server");
    }

    public static QueryExecutor connectToPresto(String prestoConfig)
    {
        return new QueryExecutor()
        {
            private final QueryExecutor delegate = testContext().getDependency(QueryExecutor.class, prestoConfig);

            @Override
            public QueryResult executeQuery(String sql, QueryParam... params)
                    throws QueryExecutionException
            {
                return Failsafe.with(ERROR_COMMITTING_WRITE_TO_HIVE_RETRY_POLICY)
                        .get(() -> delegate.executeQuery(sql, params));
            }

            @Override
            public Connection getConnection()
            {
                return delegate.getConnection();
            }

            @Override
            public void close()
            {
                delegate.close();
            }
        };
    }

    public static QueryExecutor onHive()
    {
        return testContext().getDependency(QueryExecutor.class, "hive");
    }

    public static QueryExecutor onSqlServer()
    {
        return testContext().getDependency(QueryExecutor.class, "sqlserver");
    }

    public static QueryExecutor onMySql()
    {
        return testContext().getDependency(QueryExecutor.class, "mysql");
    }

    public static QueryExecutor onSpark()
    {
        return testContext().getDependency(QueryExecutor.class, "spark");
    }

    private QueryExecutors() {}
}
