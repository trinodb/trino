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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.trino.jdbc.TrinoConnection;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;
import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;

import java.io.Closeable;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static com.google.common.cache.CacheLoader.asyncReloading;
import static java.time.Duration.ofMinutes;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class TableNameCompleter
        implements Completer, Closeable
{
    private static final long RELOAD_TIME_MINUTES = 2;

    private final ExecutorService executor = newCachedThreadPool(
            new ThreadFactoryBuilder().setNameFormat("completer-%s").setDaemon(true).build());

    private final QueryRunner queryRunner;
    private final LoadingCache<CatalogSchema, List<String>> tableCache;
    private final LoadingCache<CatalogSchema, List<String>> functionCache;
    private Optional<CatalogSchema> populatedCatalogSchema = Optional.empty();

    public TableNameCompleter(QueryRunner queryRunner)
    {
        this.queryRunner = requireNonNull(queryRunner, "queryRunner session was null!");

        tableCache = buildUnsafeCache(
                CacheBuilder.newBuilder()
                        .refreshAfterWrite(ofMinutes(RELOAD_TIME_MINUTES)),
                asyncReloading(CacheLoader.from(this::listTables), executor));

        functionCache = buildUnsafeCache(
                CacheBuilder.newBuilder(),
                CacheLoader.from(this::listFunctions));
    }

    // TODO extract safe caches implementations to a new module and use SafeCaches.buildNonEvictableCache hereAsyncCache
    @SuppressModernizer
    private static <K, V> LoadingCache<K, V> buildUnsafeCache(CacheBuilder<? super K, ? super V> cacheBuilder, CacheLoader<? super K, V> cacheLoader)
    {
        return cacheBuilder.build(cacheLoader);
    }

    private List<String> listTables(CatalogSchema catalogSchema)
    {
        return queryMetadata(connection -> {
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet tables = metadata.getTables(catalogSchema.catalog.orElse(null), escapeLikePattern(catalogSchema.schema, metadata.getSearchStringEscape()), null, null)) {
                return readColumn(tables, "TABLE_NAME");
            }
        });
    }

    private List<String> listFunctions(CatalogSchema catalogSchema)
    {
        return queryMetadata(connection -> {
            if (connection.isWrapperFor(TrinoConnection.class)) {
                // the Trino driver does not implement DatabaseMetaData.getFunctions()
                try (Statement statement = connection.createStatement();
                        ResultSet functions = statement.executeQuery("SHOW FUNCTIONS")) {
                    return readColumn(functions, 1);
                }
            }
            DatabaseMetaData metadata = connection.getMetaData();
            try (ResultSet functions = metadata.getFunctions(catalogSchema.catalog.orElse(null), escapeLikePattern(catalogSchema.schema, metadata.getSearchStringEscape()), null)) {
                return readColumn(functions, "FUNCTION_NAME");
            }
        });
    }

    private List<String> queryMetadata(QueryRunner.MetadataAction<List<String>> action)
    {
        try {
            return queryRunner.withMetadataConnection(action);
        }
        catch (SQLException | RuntimeException | AbstractMethodError e) {
            // auto-completion is best effort, and not all drivers support every metadata call
            return ImmutableList.of();
        }
    }

    private static List<String> readColumn(ResultSet resultSet, String columnLabel)
            throws SQLException
    {
        return readColumn(resultSet, resultSet.findColumn(columnLabel));
    }

    private static List<String> readColumn(ResultSet resultSet, int column)
            throws SQLException
    {
        ImmutableList.Builder<String> values = ImmutableList.builder();
        while (resultSet.next() && !Thread.currentThread().isInterrupted()) {
            String value = resultSet.getString(column);
            if (value != null) {
                values.add(value);
            }
        }
        return values.build();
    }

    private static String escapeLikePattern(String name, String escape)
    {
        if (escape == null || escape.isEmpty()) {
            return name;
        }
        return name.replace(escape, escape + escape)
                .replace("_", escape + "_")
                .replace("%", escape + "%");
    }

    public void populateCache()
    {
        Optional<CatalogSchema> catalogSchema = currentCatalogSchema();
        if (catalogSchema.isEmpty() || catalogSchema.equals(populatedCatalogSchema)) {
            return;
        }
        populatedCatalogSchema = catalogSchema;
        executor.execute(() -> {
            functionCache.refresh(catalogSchema.get());
            tableCache.refresh(catalogSchema.get());
        });
    }

    private Optional<CatalogSchema> currentCatalogSchema()
    {
        return queryRunner.getSchema()
                .map(schema -> new CatalogSchema(queryRunner.getCatalog(), schema));
    }

    @Override
    public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates)
    {
        String buffer = line.word().substring(0, line.wordCursor());
        int blankPos = findLastBlank(buffer);
        String prefix = buffer.substring(blankPos + 1);
        Optional<CatalogSchema> catalogSchema = currentCatalogSchema();

        if (catalogSchema.isEmpty()) {
            return;
        }
        List<String> functionNames = functionCache.getIfPresent(catalogSchema.get());
        List<String> tableNames = tableCache.getIfPresent(catalogSchema.get());

        if (functionNames != null) {
            for (String name : filterResults(functionNames, prefix)) {
                candidates.add(new Candidate(name));
            }
        }
        if (tableNames != null) {
            for (String name : filterResults(tableNames, prefix)) {
                candidates.add(new Candidate(name));
            }
        }
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

    private static final class CatalogSchema
    {
        private final Optional<String> catalog;
        private final String schema;

        public CatalogSchema(Optional<String> catalog, String schema)
        {
            this.catalog = requireNonNull(catalog, "catalog is null");
            this.schema = requireNonNull(schema, "schema is null");
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CatalogSchema other = (CatalogSchema) o;
            return catalog.equals(other.catalog) && schema.equals(other.schema);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(catalog, schema);
        }
    }
}
