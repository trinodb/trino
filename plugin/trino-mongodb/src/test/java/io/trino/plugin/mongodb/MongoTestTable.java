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
import io.trino.testing.sql.TemporaryRelation;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.Objects.requireNonNull;

public class MongoTestTable
        implements TemporaryRelation
{
    private final SqlExecutor sqlExecutor;
    private final String name;

    public MongoTestTable(SqlExecutor sqlExecutor, String namePrefix)
    {
        this.sqlExecutor = requireNonNull(sqlExecutor, "sqlExecutor is null");
        this.name = requireNonNull(namePrefix, "namePrefix is null") + randomNameSuffix();
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public void close()
    {
        sqlExecutor.execute("DROP TABLE " + name);
    }
}
