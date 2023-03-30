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
package io.trino.plugin.jdbc;

import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TemporaryRelation;

import static java.util.Objects.requireNonNull;

public class TestProcedure
        implements TemporaryRelation
{
    protected final SqlExecutor sqlExecutor;
    protected final String name;

    public TestProcedure(SqlExecutor sqlExecutor, String name, String createProcedureTemplate)
    {
        this.sqlExecutor = requireNonNull(sqlExecutor, "sqlExecutor is null");
        this.name = requireNonNull(name, "name is null");
        sqlExecutor.execute(createProcedureTemplate.formatted(name));
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public void close()
    {
        sqlExecutor.execute("DROP PROCEDURE " + name);
    }
}
