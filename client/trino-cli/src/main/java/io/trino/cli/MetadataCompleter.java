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
package io.trino.cli;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.trino.client.ClientSession;
import io.trino.client.StatementClient;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;
import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;

import java.io.Closeable;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.cache.CacheLoader.asyncReloading;
import static java.time.Duration.ofMinutes;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Completes catalog, schema, table, column and function names, using the
 * current session catalog and schema and the tables referenced in the statement.
 */
public class MetadataCompleter
        implements Completer, Closeable
{
    private static final long RELOAD_TIME_MINUTES = 2;
    private static final long LOAD_TIMEOUT_MILLIS = 2_000;
    private static final long MAX_CACHED_TABLES = 1_000;
    private static final String GLOBAL_KEY = "";

    private static final Pattern USE_STATEMENT = Pattern.compile("(?is)^\\s*use\\s.*");
    // the alias is matched in a lookahead so that a keyword following the table, such as JOIN, can start the next match
    private static final Pattern TABLE_REFERENCE = Pattern.compile(
            "(?i)\\b(?:from|join|update|into|table|describe)\\s+([\\w.]+)(?=(?:\\s+(?:as\\s+)?(\\w+))?)");
    private static final Set<String> NON_ALIAS_KEYWORDS = Set.of(
            "as",
            "cross",
            "except",
            "fetch",
            "for",
            "full",
            "group",
            "having",
            "inner",
            "intersect",
            "join",
            "lateral",
            "left",
            "limit",
            "natural",
            "offset",
            "on",
            "order",
            "right",
            "select",
            "set",
            "tablesample",
            "union",
            "using",
            "values",
            "where",
            "window",
            "with");

    private final ExecutorService executor = newCachedThreadPool(
            new ThreadFactoryBuilder().setNameFormat("completer-%s").setDaemon(true).build());

    private final QueryRunner queryRunner;
    private final LoadingCache<String, List<String>> catalogCache;
    private final LoadingCache<String, List<String>> functionCache;
    private final LoadingCache<String, List<String>> schemaCache;
    private final LoadingCache<SchemaName, List<String>> tableCache;
    private final LoadingCache<TableName, List<String>> columnCache;

    public MetadataCompleter(QueryRunner queryRunner)
    {
        this.queryRunner = requireNonNull(queryRunner, "queryRunner is null");

        catalogCache = buildCache(Long.MAX_VALUE, key -> queryMetadata("SHOW CATALOGS"));
        functionCache = buildCache(Long.MAX_VALUE, key -> queryMetadata("SHOW FUNCTIONS"));
        schemaCache = buildCache(Long.MAX_VALUE, catalog -> queryMetadata("SHOW SCHEMAS FROM " + quote(catalog)));
        tableCache = buildCache(MAX_CACHED_TABLES, schema -> queryMetadata("SHOW TABLES FROM " + quote(schema.catalog()) + "." + quote(schema.schema())));
        columnCache = buildCache(MAX_CACHED_TABLES, table -> queryMetadata(
                "SHOW COLUMNS FROM " + quote(table.catalog()) + "." + quote(table.schema()) + "." + quote(table.table())));
    }

    private <K> LoadingCache<K, List<String>> buildCache(long maximumSize, Function<K, List<String>> loader)
    {
        CacheBuilder<Object, Object> builder = CacheBuilder.newBuilder()
                .refreshAfterWrite(ofMinutes(RELOAD_TIME_MINUTES));
        if (maximumSize != Long.MAX_VALUE) {
            builder.maximumSize(maximumSize);
        }
        return buildUnsafeCache(builder, asyncReloading(CacheLoader.from(loader::apply), executor));
    }

    // TODO extract safe caches implementations to a new module and use SafeCaches.buildNonEvictableCache hereAsyncCache
    @SuppressModernizer
    private static <K, V> LoadingCache<K, V> buildUnsafeCache(CacheBuilder<? super K, ? super V> cacheBuilder, CacheLoader<? super K, V> cacheLoader)
    {
        return cacheBuilder.build(cacheLoader);
    }

    private List<String> queryMetadata(String query)
    {
        ImmutableList.Builder<String> names = ImmutableList.builder();
        try (StatementClient client = queryRunner.startInternalQuery(query)) {
            while (client.isRunning() && !Thread.currentThread().isInterrupted()) {
                for (List<Object> row : client.currentRows()) {
                    names.add((String) row.get(0));
                }
                client.advance();
            }
        }
        return names.build();
    }

    public void populateCache()
    {
        ClientSession session = queryRunner.getSession();
        executor.execute(() -> {
            catalogCache.refresh(GLOBAL_KEY);
            functionCache.refresh(GLOBAL_KEY);
            session.getCatalog().ifPresent(catalog -> {
                schemaCache.refresh(catalog);
                session.getSchema().ifPresent(schema -> tableCache.refresh(new SchemaName(catalog, schema)));
            });
        });
    }

    @Override
    public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates)
    {
        ClientSession session = queryRunner.getSession();
        String word = line.word().substring(0, line.wordCursor());
        String statement = currentStatement(line.line(), line.cursor());
        candidates.addAll(complete(new CachedMetadata(), session.getCatalog(), session.getSchema(), statement, word));
    }

    @VisibleForTesting
    static List<Candidate> complete(Metadata metadata, Optional<String> catalog, Optional<String> schema, String statement, String word)
    {
        int identifierStart = word.length();
        while (identifierStart > 0 && isIdentifierPart(word.charAt(identifierStart - 1))) {
            identifierStart--;
        }
        String identifier = word.substring(identifierStart);
        int lastDot = identifier.lastIndexOf('.');
        String replacementPrefix = word.substring(0, identifierStart + lastDot + 1);
        String prefix = identifier.substring(lastDot + 1).toLowerCase(ENGLISH);
        List<String> qualifier = lastDot < 0
                ? List.of()
                : Splitter.on('.').splitToList(identifier.substring(0, lastDot).toLowerCase(ENGLISH));

        boolean useStatement = USE_STATEMENT.matcher(statement).matches();
        Candidates result = new Candidates(replacementPrefix, prefix);

        switch (qualifier.size()) {
            case 0:
                result.add(metadata.catalogs(), "catalog", false);
                catalog.ifPresent(name -> result.add(metadata.schemas(name), "schema", useStatement));
                if (!useStatement) {
                    if (catalog.isPresent() && schema.isPresent()) {
                        result.add(metadata.tables(catalog.get(), schema.get()), "table", true);
                    }
                    for (TableName table : referencedTables(metadata, catalog, schema, statement).values()) {
                        result.add(metadata.columns(table), "column", true);
                    }
                    result.add(metadata.functions(), "function", true);
                }
                break;
            case 1:
                String name = qualifier.get(0);
                if (metadata.catalogs().contains(name)) {
                    result.add(metadata.schemas(name), "schema", useStatement);
                }
                if (!useStatement) {
                    if (catalog.isPresent() && metadata.schemas(catalog.get()).contains(name)) {
                        result.add(metadata.tables(catalog.get(), name), "table", true);
                    }
                    TableName table = referencedTables(metadata, catalog, schema, statement).get(name);
                    if (table != null) {
                        result.add(metadata.columns(table), "column", true);
                    }
                }
                break;
            case 2:
                if (!useStatement) {
                    String first = qualifier.get(0);
                    String second = qualifier.get(1);
                    if (metadata.catalogs().contains(first)) {
                        result.add(metadata.tables(first, second), "table", true);
                    }
                    if (catalog.isPresent() && metadata.tables(catalog.get(), first).contains(second)) {
                        result.add(metadata.columns(new TableName(catalog.get(), first, second)), "column", true);
                    }
                }
                break;
            case 3:
                if (!useStatement && metadata.tables(qualifier.get(0), qualifier.get(1)).contains(qualifier.get(2))) {
                    result.add(metadata.columns(new TableName(qualifier.get(0), qualifier.get(1), qualifier.get(2))), "column", true);
                }
                break;
            default:
                break;
        }
        return result.build();
    }

    /**
     * Returns tables referenced in the statement, keyed by alias and by unqualified table name.
     */
    private static Map<String, TableName> referencedTables(Metadata metadata, Optional<String> catalog, Optional<String> schema, String statement)
    {
        Map<String, TableName> tables = new LinkedHashMap<>();
        Matcher matcher = TABLE_REFERENCE.matcher(statement);
        while (matcher.find()) {
            List<String> parts = Splitter.on('.').splitToList(matcher.group(1).toLowerCase(ENGLISH));
            Optional<TableName> table = resolveTable(parts, catalog, schema);
            if (table.isEmpty() || table.get().table().isEmpty()
                    || !metadata.tables(table.get().catalog(), table.get().schema()).contains(table.get().table())) {
                continue;
            }
            tables.putIfAbsent(table.get().table(), table.get());
            String alias = matcher.group(2);
            if (alias != null && !NON_ALIAS_KEYWORDS.contains(alias.toLowerCase(ENGLISH))) {
                tables.put(alias.toLowerCase(ENGLISH), table.get());
            }
        }
        return tables;
    }

    private static Optional<TableName> resolveTable(List<String> parts, Optional<String> catalog, Optional<String> schema)
    {
        if (parts.size() == 1) {
            return catalog.flatMap(catalogName -> schema.map(schemaName -> new TableName(catalogName, schemaName, parts.get(0))));
        }
        if (parts.size() == 2) {
            return catalog.map(catalogName -> new TableName(catalogName, parts.get(0), parts.get(1)));
        }
        if (parts.size() == 3) {
            return Optional.of(new TableName(parts.get(0), parts.get(1), parts.get(2)));
        }
        return Optional.empty();
    }

    private static String currentStatement(String buffer, int cursor)
    {
        int start = buffer.lastIndexOf(';', Math.max(cursor - 1, 0)) + 1;
        int end = buffer.indexOf(';', cursor);
        return buffer.substring(start, end < 0 ? buffer.length() : end);
    }

    private static boolean isIdentifierPart(char c)
    {
        return Character.isLetterOrDigit(c) || c == '_' || c == '.';
    }

    private static String quote(String identifier)
    {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    @Override
    public void close()
    {
        executor.shutdownNow();
    }

    @VisibleForTesting
    interface Metadata
    {
        List<String> catalogs();

        List<String> schemas(String catalog);

        List<String> tables(String catalog, String schema);

        List<String> columns(TableName table);

        List<String> functions();
    }

    private class CachedMetadata
            implements Metadata
    {
        @Override
        public List<String> catalogs()
        {
            return load(catalogCache, GLOBAL_KEY);
        }

        @Override
        public List<String> schemas(String catalog)
        {
            return load(schemaCache, catalog);
        }

        @Override
        public List<String> tables(String catalog, String schema)
        {
            return load(tableCache, new SchemaName(catalog, schema));
        }

        @Override
        public List<String> columns(TableName table)
        {
            return load(columnCache, table);
        }

        @Override
        public List<String> functions()
        {
            // the function list is large, so never block completion on loading it
            return requireNonNullElse(functionCache.getIfPresent(GLOBAL_KEY), List.of());
        }

        private <K> List<String> load(LoadingCache<K, List<String>> cache, K key)
        {
            List<String> cached = cache.getIfPresent(key);
            if (cached != null) {
                return cached;
            }
            // a load that exceeds the timeout keeps running and fills the cache for the next completion
            Future<List<String>> future = executor.submit(() -> cache.getUnchecked(key));
            try {
                return future.get(LOAD_TIMEOUT_MILLIS, MILLISECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return List.of();
            }
            catch (ExecutionException | TimeoutException e) {
                return List.of();
            }
        }
    }

    private static class Candidates
    {
        private final String replacementPrefix;
        private final String prefix;
        private final Set<String> seen = new LinkedHashSet<>();
        private final ImmutableList.Builder<Candidate> candidates = ImmutableList.builder();

        public Candidates(String replacementPrefix, String prefix)
        {
            this.replacementPrefix = replacementPrefix;
            this.prefix = prefix;
        }

        public void add(List<String> names, String description, boolean complete)
        {
            for (String name : names) {
                String value = replacementPrefix + name;
                if (name.toLowerCase(ENGLISH).startsWith(prefix) && seen.add(value)) {
                    candidates.add(new Candidate(value, name, null, description, null, null, complete));
                }
            }
        }

        public List<Candidate> build()
        {
            return candidates.build();
        }
    }

    private static final class SchemaName
    {
        private final String catalog;
        private final String schema;

        public SchemaName(String catalog, String schema)
        {
            this.catalog = requireNonNull(catalog, "catalog is null");
            this.schema = requireNonNull(schema, "schema is null");
        }

        public String catalog()
        {
            return catalog;
        }

        public String schema()
        {
            return schema;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (!(o instanceof SchemaName)) {
                return false;
            }
            SchemaName that = (SchemaName) o;
            return catalog.equals(that.catalog) && schema.equals(that.schema);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(catalog, schema);
        }
    }

    @VisibleForTesting
    static final class TableName
    {
        private final String catalog;
        private final String schema;
        private final String table;

        public TableName(String catalog, String schema, String table)
        {
            this.catalog = requireNonNull(catalog, "catalog is null");
            this.schema = requireNonNull(schema, "schema is null");
            this.table = requireNonNull(table, "table is null");
        }

        public String catalog()
        {
            return catalog;
        }

        public String schema()
        {
            return schema;
        }

        public String table()
        {
            return table;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TableName)) {
                return false;
            }
            TableName that = (TableName) o;
            return catalog.equals(that.catalog) && schema.equals(that.schema) && table.equals(that.table);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(catalog, schema, table);
        }
    }
}
