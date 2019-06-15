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
package io.prestosql.cli;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import io.prestosql.client.QueryData;
import io.prestosql.client.StatementClient;
import jline.console.completer.Completer;

import java.io.Closeable;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.cache.CacheLoader.asyncReloading;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class TableNameCompleter
        implements Completer, Closeable
{
    private static final long RELOAD_TIME_MINUTES = 2;

    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("completer-%s"));
    private final QueryRunner queryRunner;
    private final LoadingCache<String, List<String>> tableCache;
    private final LoadingCache<String, List<String>> functionCache;

    public TableNameCompleter(QueryRunner queryRunner)
    {
        this.queryRunner = requireNonNull(queryRunner, "queryRunner session was null!");

        tableCache = CacheBuilder.newBuilder()
                .refreshAfterWrite(RELOAD_TIME_MINUTES, TimeUnit.MINUTES)
                .build(asyncReloading(CacheLoader.from(this::listTables), executor));

        functionCache = CacheBuilder.newBuilder()
                .build(asyncReloading(CacheLoader.from(this::listFunctions), executor));
    }

    private List<String> listTables(String catalogAndSchema)
    {
        if (catalogAndSchema.isEmpty()) {
            return ImmutableList.of();
        }
        int index = catalogAndSchema.indexOf(".");
        if (index >= 0) {
            String catalogName = catalogAndSchema.substring(0, index);
            String schemaName = catalogAndSchema.substring(index + 1);
            return queryMetadata(format("SELECT table_name FROM \"%s\".information_schema.tables WHERE table_schema = '%s'", catalogName.replace("\"", "\"\""), schemaName.replace("'", "''")));
        }
        else {
            String catalogName = catalogAndSchema;
            return queryMetadata(format("SELECT table_schema || '.' || table_name FROM \"%s\".information_schema.tables", catalogName.replace("\"", "\"\"")));
        }
    }

    private List<String> listFunctions(String catalogAndSchema)
    {
        return queryMetadata("SHOW FUNCTIONS");
    }

    private List<String> queryMetadata(String query)
    {
        ImmutableList.Builder<String> cache = ImmutableList.builder();
        try (StatementClient client = queryRunner.startInternalQuery(query)) {
            while (client.isRunning() && !Thread.currentThread().isInterrupted()) {
                QueryData results = client.currentData();
                if (results.getData() != null) {
                    for (List<Object> row : results.getData()) {
                        cache.add((String) row.get(0));
                    }
                }
                client.advance();
            }
        }
        return cache.build();
    }

    public void populateCache()
    {
        String key = getCatalogAndSchema();
        executor.execute(() -> {
            functionCache.refresh(key);
            tableCache.refresh(key);
        });
    }

    private String getCatalogAndSchema()
    {
        String catalogName = queryRunner.getSession().getCatalog();
        String schemaName = queryRunner.getSession().getSchema();

        StringBuilder sb = new StringBuilder();
        if (catalogName != null) {
            sb.append(catalogName);
            if (schemaName != null) {
                sb.append(".");
                sb.append(schemaName);
            }
        }

        return sb.toString();
    }

    @Override
    public int complete(String buffer, int cursor, List<CharSequence> candidates)
    {
        if (cursor <= 0) {
            return cursor;
        }
        int blankPos = findLastBlank(buffer.substring(0, cursor));
        String prefix = buffer.substring(blankPos + 1, cursor);
        String key = getCatalogAndSchema();

        List<String> functionNames = functionCache.getIfPresent(key);
        List<String> tableNames = tableCache.getIfPresent(key);

        SortedSet<String> sortedCandidates = new TreeSet<>();
        if (functionNames != null) {
            sortedCandidates.addAll(filterResults(functionNames, prefix));
        }
        if (tableNames != null) {
            sortedCandidates.addAll(filterResults(tableNames, prefix));
        }

        candidates.addAll(sortedCandidates);

        return blankPos + 1;
    }

    private static int findLastBlank(String buffer)
    {
        for (int i = buffer.length() - 1; i >= 0; i--) {
            if (Character.isWhitespace(buffer.charAt(i))) {
                return i;
            }
        }
        return -1;
    }

    private static List<String> filterResults(List<String> values, String prefix)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (String value : values) {
            if (value.startsWith(prefix)) {
                builder.add(value);
            }
        }
        return builder.build();
    }

    @Override
    public void close()
    {
        executor.shutdownNow();
    }
}
