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
package io.trino.plugin.example;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import io.airlift.json.JsonCodec;

import javax.inject.Inject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class ExampleClient
{
    /**
     * SchemaName -> (TableName -> TableMetadata)
     */
    private final Supplier<Map<String, Map<String, ExampleTable>>> schemas;

    @Inject
    public ExampleClient(ExampleConfig config, JsonCodec<Map<String, List<ExampleTable>>> catalogCodec)
    {
        requireNonNull(catalogCodec, "catalogCodec is null");
        schemas = Suppliers.memoize(schemasSupplier(catalogCodec, config.getMetadata()));
    }

    public Set<String> getSchemaNames()
    {
        return schemas.get().keySet();
    }

    public Set<String> getTableNames(String schema)
    {
        requireNonNull(schema, "schema is null");
        Map<String, ExampleTable> tables = schemas.get().get(schema);
        if (tables == null) {
            return ImmutableSet.of();
        }
        return tables.keySet();
    }

    public ExampleTable getTable(String schema, String tableName)
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");
        Map<String, ExampleTable> tables = schemas.get().get(schema);
        if (tables == null) {
            return null;
        }
        return tables.get(tableName);
    }

    private static Supplier<Map<String, Map<String, ExampleTable>>> schemasSupplier(JsonCodec<Map<String, List<ExampleTable>>> catalogCodec, URI metadataUri)
    {
        return () -> {
            try {
                return lookupSchemas(metadataUri, catalogCodec);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    private static Map<String, Map<String, ExampleTable>> lookupSchemas(URI metadataUri, JsonCodec<Map<String, List<ExampleTable>>> catalogCodec)
            throws IOException
    {
        URL result = metadataUri.toURL();
//
////        URL url = new URL("https://dev.barb-api.co.uk/api/v1/stations");
//        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
//
//        conn.setRequestProperty("Authorization","Bearer yJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoxNjc2MzQ3NjY3LCJpYXQiOjE2NzYzMDQ0NjcsImp0aSI6ImRkYzc1MGFhY2NkOTRmOTk4NzQ5M2Q4YWJiOWQ2NDBkIiwidXNlcl9pZCI6IjljMTAzNmI2LTM1NTAtNDhhYS05YjkzLTBjNjU1NGVmMjcwZCJ9.fxXZbY2mVryyZ0TjZw4-8e6vVpJmHM0sjZJ61tXlIJI");
//
//        conn.setRequestProperty("Content-Type","application/json");
//        conn.setRequestMethod("GET");
//
//
//        BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
//        String json = in.lines().collect(Collectors.joining());


        String json = Resources.toString(result, UTF_8);
        Map<String, List<ExampleTable>> catalog = catalogCodec.fromJson(json);

        return ImmutableMap.copyOf(transformValues(catalog, resolveAndIndexTables(metadataUri)));
    }

    private static Function<List<ExampleTable>, Map<String, ExampleTable>> resolveAndIndexTables(URI metadataUri)
    {
        return tables -> {
            Iterable<ExampleTable> resolvedTables = transform(tables, tableUriResolver(metadataUri));
            return ImmutableMap.copyOf(uniqueIndex(resolvedTables, ExampleTable::getName));
        };
    }

    private static Function<ExampleTable, ExampleTable> tableUriResolver(URI baseUri)
    {
        return table -> {
            List<URI> sources = ImmutableList.copyOf(transform(table.getSources(), baseUri::resolve));
            return new ExampleTable(table.getName(), table.getColumns(), sources);
        };
    }
}
