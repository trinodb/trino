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
package io.trino.metastore.biglake;

import com.google.cloud.biglake.hive.v1.HiveDatabase;
import com.google.cloud.biglake.hive.v1.HiveTable;
import com.google.cloud.biglake.hive.v1.TableName;
import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.metastore.biglake.TestingBigLakeObjects.databaseName;
import static io.trino.metastore.biglake.TestingBigLakeObjects.tableName;

/**
 * An in-memory BigLake catalog holding fake responses.
 *
 * <p>It answers the way the gRPC client does: a missing object is an empty result, and a failure
 * the client could not translate away is a {@link TrinoException}.
 */
public final class TestingBigLakeClient
        implements BigLakeClient
{
    private final Map<String, HiveDatabase> databases = new LinkedHashMap<>();
    private final Map<String, List<HiveTable>> tables = new LinkedHashMap<>();
    private TrinoException failure;

    public TestingBigLakeClient add(HiveDatabase database)
    {
        databases.put(databaseName(database), database);
        return this;
    }

    public TestingBigLakeClient add(HiveTable table)
    {
        tables.computeIfAbsent(databaseOf(table), _ -> new ArrayList<>()).add(table);
        return this;
    }

    /**
     * Makes every read fail, the way BigLake reports a denied or broken call.
     */
    public TestingBigLakeClient failWith(TrinoException failure)
    {
        this.failure = failure;
        return this;
    }

    @Override
    public List<HiveDatabase> listDatabases()
    {
        throwIfFailing();
        return ImmutableList.copyOf(databases.values());
    }

    @Override
    public Optional<HiveDatabase> getDatabase(String databaseName)
    {
        throwIfFailing();
        return Optional.ofNullable(databases.get(databaseName));
    }

    @Override
    public List<HiveTable> listTables(String databaseName)
    {
        throwIfFailing();
        return ImmutableList.copyOf(tables.getOrDefault(databaseName, ImmutableList.of()));
    }

    @Override
    public Optional<HiveTable> getTable(String databaseName, String tableName)
    {
        throwIfFailing();
        return listTables(databaseName).stream()
                .filter(table -> tableName.equals(tableName(table)))
                .findFirst();
    }

    private static String databaseOf(HiveTable table)
    {
        return TableName.parse(table.getName()).getDatabase();
    }

    private void throwIfFailing()
    {
        if (failure != null) {
            throw failure;
        }
    }
}
