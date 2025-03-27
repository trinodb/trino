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
package io.trino.plugin.sqlserver;

import io.trino.testing.sql.SqlExecutor;

import java.io.Closeable;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;

public class TestSynonym
        implements Closeable
{
    private final SqlExecutor sqlExecutor;
    private final String name;

    public TestSynonym(SqlExecutor sqlExecutor, String namePrefix, String definition)
    {
        this.sqlExecutor = sqlExecutor;
        this.name = namePrefix + "_" + randomNameSuffix();
        sqlExecutor.execute(format("CREATE SYNONYM %s %s", name, definition));
    }

    public String getName()
    {
        return name;
    }

    @Override
    public void close()
    {
        sqlExecutor.execute("DROP SYNONYM " + name);
    }
}
