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
package io.trino.tests.product.hive.util;

import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static java.util.Objects.requireNonNull;

public class TemporaryHiveTable
        implements AutoCloseable
{
    public static TemporaryHiveTable temporaryHiveTable(String tableName)
    {
        return new TemporaryHiveTable(tableName);
    }

    private final String name;

    private TemporaryHiveTable(String name)
    {
        this.name = requireNonNull(name, "name is null");
    }

    public String getName()
    {
        return name;
    }

    public void closeQuietly(Exception e)
    {
        try (TemporaryHiveTable justCloseIt = this) {
            // suppress empty try-catch warning
        }
        catch (Exception closeException) {
            // Self-suppression not permitted
            if (e != closeException) {
                e.addSuppressed(closeException);
            }
        }
    }

    @Override
    public void close()
    {
        onHive().executeQuery("DROP TABLE " + name);
    }
}
