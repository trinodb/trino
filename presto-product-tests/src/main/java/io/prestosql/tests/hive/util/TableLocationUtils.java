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
package io.prestosql.tests.hive.util;

import java.net.URI;
import java.net.URISyntaxException;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.tests.utils.QueryExecutors.onPresto;
import static java.lang.String.format;

public final class TableLocationUtils
{
    private TableLocationUtils()
    {
    }

    public static String getTableLocation(String tableName)
    {
        return getTableLocation(tableName, 0);
    }

    public static String getTableLocation(String tableName, int partitionColumns)
    {
        StringBuilder regex = new StringBuilder("/[^/]*$");
        for (int i = 0; i < partitionColumns; i++) {
            regex.insert(0, "/[^/]*");
        }
        return getOnlyElement(onPresto().executeQuery(format("SELECT DISTINCT regexp_replace(\"$path\", '%s', '') FROM %s", regex.toString(), tableName)).column(1));
    }

    public static String getTablePath(String tableName, int partitionColumns)
            throws URISyntaxException
    {
        String location = getTableLocation(tableName, partitionColumns);
        URI uri = new URI(location);
        return uri.getPath();
    }
}
