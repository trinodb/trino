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
package io.trino.metastore.lakehouse;

import com.google.cloud.biglake.hive.v1.HiveDatabase;
import com.google.cloud.biglake.hive.v1.HiveTable;
import com.google.cloud.biglake.hive.v1.Partition;
import com.google.cloud.biglake.hive.v1.TableName;
import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metastore.lakehouse.TestingLakehouseObjects.databaseName;
import static io.trino.metastore.lakehouse.TestingLakehouseObjects.tableName;

/**
 * An in-memory Lakehouse catalog holding fake responses.
 *
 * <p>It answers the way the gRPC client does: a missing object is an empty result, and a failure
 * the client could not translate away is a {@link TrinoException}.
 */
public final class TestingLakehouseClient
        implements LakehouseClient
{
    private final Map<String, HiveDatabase> databases = new LinkedHashMap<>();
    private final Map<String, List<HiveTable>> tables = new LinkedHashMap<>();
    private final Map<String, List<Partition>> partitions = new LinkedHashMap<>();
    private final List<String> partitionFilters = new ArrayList<>();
    private TrinoException failure;

    public TestingLakehouseClient add(HiveDatabase database)
    {
        databases.put(databaseName(database), database);
        return this;
    }

    public TestingLakehouseClient add(HiveTable table)
    {
        tables.computeIfAbsent(databaseOf(table), _ -> new ArrayList<>()).add(table);
        return this;
    }

    public TestingLakehouseClient add(String databaseName, String tableName, Partition partition)
    {
        partitions.computeIfAbsent(databaseName + "." + tableName, _ -> new ArrayList<>()).add(partition);
        return this;
    }

    /**
     * Makes every read fail, the way Lakehouse reports a denied or broken call.
     */
    public TestingLakehouseClient failWith(TrinoException failure)
    {
        this.failure = failure;
        return this;
    }

    /**
     * Returns the filter of every partition listing, so a test can assert what was pushed down.
     */
    public List<String> partitionFilters()
    {
        return ImmutableList.copyOf(partitionFilters);
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

    @Override
    public Optional<List<Partition>> listPartitions(String databaseName, String tableName, String filter)
    {
        throwIfFailing();
        if (getTable(databaseName, tableName).isEmpty()) {
            return Optional.empty();
        }
        partitionFilters.add(filter);

        List<Partition> known = partitions.getOrDefault(databaseName + "." + tableName, ImmutableList.of());
        if (filter.isEmpty()) {
            return Optional.of(ImmutableList.copyOf(known));
        }
        return Optional.of(known.stream()
                .filter(partition -> matches(partition, filter))
                .collect(toImmutableList()));
    }

    /**
     * Matches the value of a single equality, which is all the adapter ever builds for one column.
     */
    private static boolean matches(Partition partition, String filter)
    {
        for (String condition : filter.split(" and ")) {
            int quote = condition.indexOf('\'');
            if (quote < 0) {
                continue;
            }
            String value = condition.substring(quote + 1, condition.lastIndexOf('\''));
            if (!partition.getValuesList().contains(value)) {
                return false;
            }
        }
        return true;
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
