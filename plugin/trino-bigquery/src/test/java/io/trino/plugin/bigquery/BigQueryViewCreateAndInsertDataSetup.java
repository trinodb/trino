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
import io.trino.testing.sql.TemporaryRelation;

import java.util.List;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
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
    public TemporaryRelation setupTemporaryRelation(List<ColumnSetup> inputs)
    {
        TemporaryRelation table = super.setupTemporaryRelation(inputs);
        try {
            return new BigQueryTestView(sqlExecutor, table);
        }
        catch (Throwable e) {
            closeAllSuppress(e, table);
            throw e;
        }
    }
}
