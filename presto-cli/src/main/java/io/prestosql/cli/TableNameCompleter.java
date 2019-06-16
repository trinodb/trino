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
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
    private final LoadingCache<String, List<TableNameCompletionProposal>> tableCache;
    private final LoadingCache<String, List<String>> functionCache;
    private final LoadingCache<String, List<String>> catalogCache;

    private static class TableNameCompletionProposal
    {
        private final String catalogName;
        private final String schemaName;
        private final String tableName;

        public TableNameCompletionProposal(String catalogName, String schemaName, String tableName)
        {
            this.catalogName = catalogName;
            this.schemaName = schemaName;
            this.tableName = tableName;
        }

        public String withCatalogName()
        {
            return catalogName + "." + schemaName + "." + tableName;
        }

        public String withSchemaName()
        {
            return schemaName + "." + tableName;
        }

        public String simpleName()
        {
            return tableName;
        }

        public boolean belongToCatalog(String catalogName)
        {
            return this.catalogName.equals(catalogName);
        }

        public boolean belongToSchema(String catalogName, String schemaName)
        {
            return belongToCatalog(catalogName) && this.schemaName.equals(schemaName);
        }
    }

    public TableNameCompleter(QueryRunner queryRunner)
    {
        this.queryRunner = requireNonNull(queryRunner, "queryRunner session was null!");

        tableCache = CacheBuilder.newBuilder()
                .refreshAfterWrite(RELOAD_TIME_MINUTES, TimeUnit.MINUTES)
                .build(asyncReloading(CacheLoader.from(this::listTables), executor));

        functionCache = CacheBuilder.newBuilder()
                .build(asyncReloading(CacheLoader.from(this::listFunctions), executor));

        catalogCache = CacheBuilder.newBuilder()
                .build(asyncReloading(CacheLoader.from(this::listCatalogs), executor));
    }

    private List<TableNameCompletionProposal> listTables(String catalogName)
    {
        if (catalogName.isEmpty()) {
            return ImmutableList.of();
        }

        String sql = "SELECT table_catalog, table_schema, table_name FROM \"%s\".information_schema.tables";
        return queryMetadata(format(sql, catalogName.replace("\"", "\"\"")), 3).stream().map(row ->
                new TableNameCompletionProposal(row.get(0), row.get(1), row.get(2))).collect(Collectors.toList());
    }

    private List<String> listFunctions(String catalogName)
    {
        return queryMetadata("SHOW FUNCTIONS", 1).stream().map(row -> row.get(0)).collect(Collectors.toList());
    }

    private List<String> listCatalogs(String catalogName)
    {
        return queryMetadata("SHOW CATALOGS", 1).stream().map(row -> row.get(0)).collect(Collectors.toList());
    }

    private List<List<String>> queryMetadata(String query, int columns)
    {
        ImmutableList.Builder<List<String>> cache = ImmutableList.builder();
        try (StatementClient client = queryRunner.startInternalQuery(query)) {
            while (client.isRunning() && !Thread.currentThread().isInterrupted()) {
                QueryData results = client.currentData();
                if (results.getData() != null) {
                    for (List<Object> row : results.getData()) {
                        List<String> list = new ArrayList<>();
                        for (int i = 0; i < columns; i++) {
                            list.add((String) row.get(i));
                        }
                        cache.add(ImmutableList.copyOf(list));
                    }
                }
                client.advance();
            }
        }
        return cache.build();
    }

    public void populateCache()
    {
        String catalogName = getCatalogName("");
        executor.execute(() -> {
            functionCache.refresh(catalogName);
            tableCache.refresh(catalogName);
            catalogCache.refresh("");
        });
    }

    @Override
    public int complete(String buffer, int cursor, List<CharSequence> candidates)
    {
        if (cursor <= 0) {
            return cursor;
        }
        int blankPos = findLastBlank(buffer.substring(0, cursor));
        String prefix = buffer.substring(blankPos + 1, cursor);
        String catalogName = getCatalogName(prefix);

        List<String> functionNames = functionCache.getIfPresent(catalogName);
        List<TableNameCompletionProposal> tableNames = tableCache.getIfPresent(catalogName);

        // Try to refresh on demand
        if (tableNames == null) {
            tableCache.refresh(catalogName);
            tableNames = tableCache.getIfPresent(catalogName);
        }

        SortedSet<String> sortedCandidates = new TreeSet<>();
        if (functionNames != null) {
            sortedCandidates.addAll(filterResults(functionNames, prefix));
        }
        if (tableNames != null) {
            String sessionCatalog = queryRunner.getSession().getCatalog();
            String sessionSchema = queryRunner.getSession().getSchema();
            List<String> adjustedTableNames = new ArrayList<>();

            tableNames.forEach(tableName -> {
                if (prefix.contains(".")) {
                    adjustedTableNames.add(tableName.withCatalogName());
                }
                if (tableName.belongToCatalog(sessionCatalog)) {
                    adjustedTableNames.add(tableName.withSchemaName());
                }
                if (tableName.belongToSchema(sessionCatalog, sessionSchema)) {
                    adjustedTableNames.add(tableName.simpleName());
                }
            });

            sortedCandidates.addAll(filterResults(ImmutableList.copyOf(adjustedTableNames), prefix));
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

    private String getCatalogName(String prefix)
    {
        String[] fragments = splitTableName(prefix);
        if (fragments.length > 1) {
            List<String> catalogs = catalogCache.getIfPresent("");
            if (catalogs.contains(fragments[0])) {
                return fragments[0];
            }
        }
        String catalogName = queryRunner.getSession().getCatalog();
        if (catalogName != null) {
            return catalogName;
        }
        return "";
    }

    private String[] splitTableName(String tableName)
    {
        List<String> fragments = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < tableName.length(); i++) {
            char c = tableName.charAt(i);
            if (c == '.') {
                fragments.add(sb.toString());
                sb = new StringBuilder();
            }
            else {
                sb.append(c);
            }
        }
        if (sb.length() > 0) {
            fragments.add(sb.toString());
        }
        else if (tableName.endsWith(".")) {
            fragments.add("");
        }

        return fragments.toArray(new String[fragments.size()]);
    }

    @Override
    public void close()
    {
        executor.shutdownNow();
    }
}
