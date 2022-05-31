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
package io.trino.plugin.mongodb;

import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class MongoTestTable
        extends TestTable
{
    public MongoTestTable(SqlExecutor sqlExecutor, String namePrefix)
    {
        super(sqlExecutor, namePrefix, null);
    }

    @Override
    public void createAndInsert(List<String> rowsToInsert)
    {
        checkArgument(rowsToInsert.isEmpty(), "rowsToInsert must be empty");
    }
}
