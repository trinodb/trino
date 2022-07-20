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

import io.trino.testing.datatype.ColumnSetup;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class BigQueryViewCreateAndInsertDataSetup
        extends CreateAndInsertDataSetup
{
    private final SqlExecutor sqlExecutor;

    public BigQueryViewCreateAndInsertDataSetup(SqlExecutor sqlExecutor, String tableNamePrefix)
    {
        super(sqlExecutor, tableNamePrefix);
        this.sqlExecutor = requireNonNull(sqlExecutor, "sqlExecutor is null");
    }

    @Override
    public TestTable setupTemporaryRelation(List<ColumnSetup> inputs)
    {
        TestTable table = super.setupTemporaryRelation(inputs);
        BigQueryTestView view = new BigQueryTestView(sqlExecutor, table);
        view.createView();
        return view;
    }
}
