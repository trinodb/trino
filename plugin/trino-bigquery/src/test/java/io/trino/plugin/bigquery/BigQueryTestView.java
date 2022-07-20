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
package io.trino.plugin.bigquery;

import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;

import java.util.List;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class BigQueryTestView
        extends TestTable
{
    private final TestTable table;
    private final String viewName;

    public BigQueryTestView(SqlExecutor sqlExecutor, TestTable table)
    {
        super(sqlExecutor, table.getName(), null);
        this.table = requireNonNull(table, "table is null");
        this.viewName = table.getName() + "_view";
    }

    @Override
    public void createAndInsert(List<String> rowsToInsert) {}

    public void createView()
    {
        sqlExecutor.execute(format("CREATE VIEW %s AS SELECT * FROM %s", viewName, table.getName()));
    }

    @Override
    public String getName()
    {
        return viewName;
    }

    @Override
    public void close()
    {
        sqlExecutor.execute("DROP TABLE " + table.getName());
        sqlExecutor.execute("DROP VIEW " + viewName);
    }
}
