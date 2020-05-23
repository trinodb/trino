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
package io.prestosql.testing;

import com.google.common.collect.ImmutableList;
import io.prestosql.connector.MockConnectorFactory;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.SchemaTableName;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.connector.MockConnectorFactory.Builder.defaultGetColumns;

public class CountingMockConnector
{
    private final Object lock = new Object();

    private final List<SchemaTableName> tablesTestSchema1 = IntStream.range(0, 1000)
            .mapToObj(i -> new SchemaTableName("test_schema1", "test_table" + i))
            .collect(toImmutableList());

    private final List<SchemaTableName> tablesTestSchema2 = IntStream.range(0, 2000)
            .mapToObj(i -> new SchemaTableName("test_schema2", "test_table" + i))
            .collect(toImmutableList());

    private final AtomicLong listSchemasCallsCounter = new AtomicLong();
    private final AtomicLong listTablesCallsCounter = new AtomicLong();
    private final AtomicLong getColumnsCallsCounter = new AtomicLong();

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
                tablesTestSchema1.stream(),
                tablesTestSchema2.stream());
    }

    public MetadataCallsCount runCounting(Runnable runnable)
    {
        synchronized (lock) {
            listSchemasCallsCounter.set(0);
            listTablesCallsCounter.set(0);
            getColumnsCallsCounter.set(0);

            runnable.run();

            return new MetadataCallsCount()
                    .withListSchemasCount(listSchemasCallsCounter.get())
                    .withListTablesCount(listTablesCallsCounter.get())
                    .withGetColumnsCount(getColumnsCallsCounter.get());
        }
    }

    private ConnectorFactory getConnectorFactory()
    {
        MockConnectorFactory mockConnectorFactory = MockConnectorFactory.builder()
                .withListSchemaNames(connectorSession -> {
                    listSchemasCallsCounter.incrementAndGet();
                    return ImmutableList.of("test_schema1", "test_schema2");
                })
                .withListTables((connectorSession, schemaName) -> {
                    listTablesCallsCounter.incrementAndGet();
                    if (schemaName.equals("test_schema1")) {
                        return tablesTestSchema1;
                    }
                    if (schemaName.equals("test_schema2")) {
                        return tablesTestSchema2;
                    }
                    return ImmutableList.of();
                })
                .withGetColumns(schemaTableName -> {
                    getColumnsCallsCounter.incrementAndGet();
                    return defaultGetColumns().apply(schemaTableName);
                })
                .build();

        return mockConnectorFactory;
    }

    public static final class MetadataCallsCount
    {
        private final long listSchemasCount;
        private final long listTablesCount;
        private final long getColumnsCount;

        public MetadataCallsCount()
        {
            this(0, 0, 0);
        }

        public MetadataCallsCount(long listSchemasCount, long listTablesCount, long getColumnsCount)
        {
            this.listSchemasCount = listSchemasCount;
            this.listTablesCount = listTablesCount;
            this.getColumnsCount = getColumnsCount;
        }

        public MetadataCallsCount withListSchemasCount(long listSchemasCount)
        {
            return new MetadataCallsCount(listSchemasCount, listTablesCount, getColumnsCount);
        }

        public MetadataCallsCount withListTablesCount(long listTablesCount)
        {
            return new MetadataCallsCount(listSchemasCount, listTablesCount, getColumnsCount);
        }

        public MetadataCallsCount withGetColumnsCount(long getColumnsCount)
        {
            return new MetadataCallsCount(listSchemasCount, listTablesCount, getColumnsCount);
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
            MetadataCallsCount that = (MetadataCallsCount) o;
            return listSchemasCount == that.listSchemasCount &&
                    listTablesCount == that.listTablesCount &&
                    getColumnsCount == that.getColumnsCount;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(listSchemasCount, listTablesCount, getColumnsCount);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("listSchemasCount", listSchemasCount)
                    .add("listTablesCount", listTablesCount)
                    .add("getColumnsCount", getColumnsCount)
                    .toString();
        }
    }
}
