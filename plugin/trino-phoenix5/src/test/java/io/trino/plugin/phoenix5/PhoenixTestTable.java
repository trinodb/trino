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
package io.trino.plugin.phoenix5;

import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;

import java.util.List;

import static java.lang.String.format;

public class PhoenixTestTable
        extends TestTable
{
    public PhoenixTestTable(SqlExecutor sqlExecutor, String namePrefix, String tableDefinition, List<String> rowsToInsert)
    {
        super(sqlExecutor, namePrefix, tableDefinition, rowsToInsert);
    }

    @Override
    protected void createAndInsert(List<String> rowsToInsert)
    {
        sqlExecutor.execute(format("CREATE TABLE %s %s", name, tableDefinition));
        try {
            for (String row : rowsToInsert) {
                sqlExecutor.execute(format("UPSERT INTO %s VALUES (%s)", name, row));
            }
        }
        catch (Exception e) {
            try (PhoenixTestTable ignored = this) {
                throw e;
            }
        }
    }
}
