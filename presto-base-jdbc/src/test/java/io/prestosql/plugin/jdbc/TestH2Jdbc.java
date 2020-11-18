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
package io.prestosql.plugin.jdbc;

import com.google.common.collect.ImmutableList;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.sql.JdbcSqlExecutor;
import io.prestosql.tpch.TpchTable;

import java.util.Map;
import java.util.Properties;

import static io.prestosql.plugin.jdbc.H2QueryRunner.createH2QueryRunner;

public class TestH2Jdbc
        extends BaseH2JdbcTest
{
    private final Map<String, String> properties = TestingH2JdbcModule.createProperties();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createH2QueryRunner(ImmutableList.copyOf(TpchTable.getTables()), properties);
    }

    @Override
    protected JdbcSqlExecutor getSqlExecutor()
    {
        return new JdbcSqlExecutor(properties.get("connection-url"), new Properties());
    }
}
