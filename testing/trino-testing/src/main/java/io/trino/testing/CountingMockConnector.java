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
package io.trino.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multiset;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.trino.connector.MockConnectorFactory;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.tracing.TracingConnectorMetadata;
import io.trino.util.AutoCloseableCloser;

import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableMultiset.toImmutableMultiset;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.connector.MockConnectorFactory.Builder.defaultGetColumns;
import static io.trino.connector.MockConnectorFactory.Builder.defaultGetTableHandle;
import static io.trino.spi.security.PrincipalType.USER;
import static java.util.Map.entry;
import static java.util.stream.Collectors.joining;

public class CountingMockConnector
        implements AutoCloseable
{
    private final Object lock = new Object();

    private final Set<String> tablesTestSchema1 = IntStream.range(0, 1000)
            .mapToObj(i -> "test_table" + i)
            .collect(toImmutableSet());

    private final Set<String> tablesTestSchema2 = IntStream.range(0, 2000)
            .mapToObj(i -> "test_table" + i)
            .collect(toImmutableSet());

    private final Set<RoleGrant> roleGrants = IntStream.range(0, 100)
            .mapToObj(i -> new RoleGrant(new TrinoPrincipal(USER, "user" + (i == 0 ? "" : i)), "role" + i / 2, false))
            .collect(toImmutableSet());

    private final AutoCloseableCloser closer = AutoCloseableCloser.create();

    private final InMemorySpanExporter spanExporter;
    private final SdkTracerProvider tracerProvider;

    public CountingMockConnector()
    {
        spanExporter = closer.register(InMemorySpanExporter.create());
        tracerProvider = closer.register(SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                .build());
    }

    @Override
    public void close()
            throws Exception
    {
        closer.close();
    }

    public Plugin getPlugin()
    {
        return new Plugin()
        {
            @Override
            public Iterable<ConnectorFactory> getConnectorFactories()
            {
                return ImmutableList.of(getConnectorFactory());
            }
        };
    }

    public Stream<SchemaTableName> getAllTables()
    {
        return Stream.concat(
                tablesTestSchema1.stream()
                        .map(tableName -> new SchemaTableName("test_schema1", tableName)),
                tablesTestSchema2.stream()
                        .map(tableName -> new SchemaTableName("test_schema2", tableName)));
    }

    public Multiset<String> runTracing(Runnable runnable)
    {
        synchronized (lock) {
            spanExporter.reset();

            runnable.run();

            return spanExporter.getFinishedSpanItems().stream()
                    .map(span -> {
                        String attributes = span.getAttributes().asMap().entrySet().stream()
                                .map(entry -> entry(entry.getKey().getKey(), entry.getValue()))
                                .filter(entry -> !entry.getKey().equals("trino.catalog"))
                                .map(entry -> "%s=%s".formatted(entry.getKey().replaceFirst("^trino\\.", ""), entry.getValue()))
                                .sorted()
                                .collect(joining(", "));
                        if (attributes.isEmpty()) {
                            return span.getName();
                        }
                        return "%s(%s)".formatted(span.getName(), attributes);
                    })
                    .collect(toImmutableMultiset());
        }
    }

    private ConnectorFactory getConnectorFactory()
    {
        MockConnectorFactory mockConnectorFactory = MockConnectorFactory.builder()
                .withMetadataWrapper(connectorMetadata -> new TracingConnectorMetadata(tracerProvider.get("test"), "mock", connectorMetadata))
                .withListSchemaNames(connectorSession -> ImmutableList.of("test_schema1", "test_schema2"))
                .withListTables((connectorSession, schemaName) -> {
                    if (schemaName.equals("test_schema1")) {
                        return ImmutableList.copyOf(tablesTestSchema1);
                    }
                    if (schemaName.equals("test_schema2")) {
                        return ImmutableList.copyOf(tablesTestSchema2);
                    }
                    return ImmutableList.of();
                })
                .withGetTableHandle((connectorSession, schemaTableName) -> {
                    switch (schemaTableName.getSchemaName()) {
                        case "test_schema1" -> {
                            if (!tablesTestSchema1.contains(schemaTableName.getTableName())) {
                                return null;
                            }
                        }
                        case "test_schema2" -> {
                            if (!tablesTestSchema2.contains(schemaTableName.getTableName())) {
                                return null;
                            }
                        }
                        default -> {
                            return null;
                        }
                    }
                    return defaultGetTableHandle().apply(connectorSession, schemaTableName);
                })
                .withGetColumns(schemaTableName -> defaultGetColumns().apply(schemaTableName))
                .withListRoleGrants((connectorSession, roles, grantees, limit) -> roleGrants)
                .build();

        return mockConnectorFactory;
    }
}
