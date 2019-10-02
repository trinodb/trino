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
package io.prestosql.tests.sql;

import java.security.SecureRandom;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Character.MAX_RADIX;
import static java.lang.Math.abs;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TestTable
        implements AutoCloseable
{
    private static final SecureRandom random = new SecureRandom();
    private static final int RANDOM_SUFFIX_LENGTH = 12;

    private final SqlExecutor sqlExecutor;
    private final String name;

    public TestTable(SqlExecutor sqlExecutor, String namePrefix, String tableDefinition)
    {
        checkArgument(!tableDefinition.contains("{TABLE_NAME}"), "tableDefinition should not contain '{TABLE_NAME}': %s", tableDefinition);
        this.sqlExecutor = requireNonNull(sqlExecutor, "sqlExecutor is null");
        requireNonNull(namePrefix, "namePrefix is null");
        requireNonNull(tableDefinition, "tableDefinition is null");
        this.name = namePrefix + "_" + randomTableSuffix();
        sqlExecutor.execute(format("CREATE TABLE %s %s", name, tableDefinition));
    }

    public String getName()
    {
        return name;
    }

    @Override
    public void close()
    {
        sqlExecutor.execute("DROP TABLE " + name);
    }

    private static String randomTableSuffix()
    {
        String randomSuffix = Long.toString(abs(random.nextLong()), MAX_RADIX);
        return randomSuffix.substring(0, min(RANDOM_SUFFIX_LENGTH, randomSuffix.length()));
    }
}
