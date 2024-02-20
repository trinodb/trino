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
import io.trino.client.QueryData;
import io.trino.client.StatementClient;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;
import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;

import java.io.Closeable;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.cache.CacheLoader.asyncReloading;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class TableNameCompleter
        implements Completer, Closeable
{
    private static final long RELOAD_TIME_MINUTES = 2;

    private final ExecutorService executor = newCachedThreadPool(
            new ThreadFactoryBuilder().setNameFormat("completer-%s").setDaemon(true).build());

    private final QueryRunner queryRunner;
    private final LoadingCache<String, List<String>> tableCache;
    private final LoadingCache<String, List<String>> functionCache;

    public TableNameCompleter(QueryRunner queryRunner)
    {
        this.queryRunner = requireNonNull(queryRunner, "queryRunner session was null!");

        tableCache = buildUnsafeCache(
                CacheBuilder.newBuilder()
                        .refreshAfterWrite(RELOAD_TIME_MINUTES, TimeUnit.MINUTES),
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

    private List<String> listTables(String schemaName)
    {
        return queryMetadata(format("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'", schemaName));
    }

    @SuppressWarnings("unused")
    private List<String> listFunctions(String schemaName)
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
        queryRunner.getSession().getSchema().ifPresent(schemaName -> executor.execute(() -> {
            functionCache.refresh(schemaName);
            tableCache.refresh(schemaName);
        }));
    }

    @Override
    public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates)
    {
        String buffer = line.word().substring(0, line.wordCursor());
        int blankPos = findLastBlank(buffer);
        String prefix = buffer.substring(blankPos + 1);
        Optional<String> schemaName = queryRunner.getSession().getSchema();

        if (!schemaName.isPresent()) {
            return;
        }
        List<String> functionNames = functionCache.getIfPresent(schemaName.get());
        List<String> tableNames = tableCache.getIfPresent(schemaName.get());

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
}
