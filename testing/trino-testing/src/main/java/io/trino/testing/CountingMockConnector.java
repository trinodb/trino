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
import io.trino.connector.MockConnectorFactory;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.TrinoPrincipal;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.connector.MockConnectorFactory.Builder.defaultGetColumns;
import static io.trino.connector.MockConnectorFactory.Builder.defaultGetTableHandle;
import static io.trino.spi.security.PrincipalType.USER;

public class CountingMockConnector
{
    private final Object lock = new Object();

    private final List<String> tablesTestSchema1 = IntStream.range(0, 1000)
            .mapToObj(i -> "test_table" + i)
            .collect(toImmutableList());

    private final List<String> tablesTestSchema2 = IntStream.range(0, 2000)
            .mapToObj(i -> "test_table" + i)
            .collect(toImmutableList());

    private final Set<RoleGrant> roleGrants = IntStream.range(0, 100)
            .mapToObj(i -> new RoleGrant(new TrinoPrincipal(USER, "user" + (i == 0 ? "" : i)), "role" + i / 2, false))
            .collect(toImmutableSet());

    private final AtomicLong listSchemasCallsCounter = new AtomicLong();
    private final AtomicLong listTablesCallsCounter = new AtomicLong();
    private final AtomicLong getTableHandleCallsCounter = new AtomicLong();
    private final AtomicLong getColumnsCallsCounter = new AtomicLong();
    private final ListRoleGrantsCounter listRoleGranstCounter = new ListRoleGrantsCounter();

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

    public MetadataCallsCount runCounting(Runnable runnable)
    {
        synchronized (lock) {
            listSchemasCallsCounter.set(0);
            listTablesCallsCounter.set(0);
            getTableHandleCallsCounter.set(0);
            getColumnsCallsCounter.set(0);
            listRoleGranstCounter.reset();

            runnable.run();

            return new MetadataCallsCount(
                    listSchemasCallsCounter.get(),
                    listTablesCallsCounter.get(),
                    getTableHandleCallsCounter.get(),
                    getColumnsCallsCounter.get(),
                    listRoleGranstCounter.listRowGrantsCallsCounter.get(),
                    listRoleGranstCounter.rolesPushedCounter.get(),
                    listRoleGranstCounter.granteesPushedCounter.get(),
                    listRoleGranstCounter.limitPushedCounter.get());
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
                .withGetTableHandle((connectorSession, schemaTableName) -> {
                    getTableHandleCallsCounter.incrementAndGet();
                    return defaultGetTableHandle().apply(connectorSession, schemaTableName);
                })
                .withGetColumns(schemaTableName -> {
                    getColumnsCallsCounter.incrementAndGet();
                    return defaultGetColumns().apply(schemaTableName);
                })
                .withListRoleGrants((connectorSession, roles, grantees, limit) -> {
                    listRoleGranstCounter.incrementListRoleGrants(roles, grantees, limit);
                    return roleGrants;
                })
                .build();

        return mockConnectorFactory;
    }

    public static final class MetadataCallsCount
    {
        private final long listSchemasCount;
        private final long listTablesCount;
        private final long getTableHandleCount;
        private final long getColumnsCount;
        private final long listRoleGrantsCount;
        private final long rolesPushedCount;
        private final long granteesPushedCount;
        private final long limitPushedCount;

        public MetadataCallsCount()
        {
            this(0, 0, 0, 0, 0, 0, 0, 0);
        }

        public MetadataCallsCount(
                long listSchemasCount,
                long listTablesCount,
                long getTableHandleCount,
                long getColumnsCount,
                long listRoleGrantsCount,
                long rolesPushedCount,
                long granteesPushedCount,
                long limitPushedCount)
        {
            this.listSchemasCount = listSchemasCount;
            this.listTablesCount = listTablesCount;
            this.getTableHandleCount = getTableHandleCount;
            this.getColumnsCount = getColumnsCount;
            this.listRoleGrantsCount = listRoleGrantsCount;
            this.rolesPushedCount = rolesPushedCount;
            this.granteesPushedCount = granteesPushedCount;
            this.limitPushedCount = limitPushedCount;
        }

        public MetadataCallsCount withListSchemasCount(long listSchemasCount)
        {
            return new MetadataCallsCount(
                    listSchemasCount,
                    listTablesCount,
                    getTableHandleCount,
                    getColumnsCount,
                    listRoleGrantsCount,
                    rolesPushedCount,
                    granteesPushedCount,
                    limitPushedCount);
        }

        public MetadataCallsCount withListTablesCount(long listTablesCount)
        {
            return new MetadataCallsCount(
                    listSchemasCount,
                    listTablesCount,
                    getTableHandleCount,
                    getColumnsCount,
                    listRoleGrantsCount,
                    rolesPushedCount,
                    granteesPushedCount,
                    limitPushedCount);
        }

        public MetadataCallsCount withGetTableHandleCount(long getTableHandleCount)
        {
            return new MetadataCallsCount(
                    listSchemasCount,
                    listTablesCount,
                    getTableHandleCount,
                    getColumnsCount,
                    listRoleGrantsCount,
                    rolesPushedCount,
                    granteesPushedCount,
                    limitPushedCount);
        }

        public MetadataCallsCount withGetColumnsCount(long getColumnsCount)
        {
            return new MetadataCallsCount(
                    listSchemasCount,
                    listTablesCount,
                    getTableHandleCount,
                    getColumnsCount,
                    listRoleGrantsCount,
                    rolesPushedCount,
                    granteesPushedCount,
                    limitPushedCount);
        }

        public MetadataCallsCount withListRoleGrantsCount(long listRoleGrantsCount)
        {
            return new MetadataCallsCount(
                    listSchemasCount,
                    listTablesCount,
                    getTableHandleCount,
                    getColumnsCount,
                    listRoleGrantsCount,
                    rolesPushedCount,
                    granteesPushedCount,
                    limitPushedCount);
        }

        public MetadataCallsCount withRolesPushedCount(long rolesPushedCount)
        {
            return new MetadataCallsCount(
                    listSchemasCount,
                    listTablesCount,
                    getTableHandleCount,
                    getColumnsCount,
                    listRoleGrantsCount,
                    rolesPushedCount,
                    granteesPushedCount,
                    limitPushedCount);
        }

        public MetadataCallsCount withGranteesPushedCount(long granteesPushedCount)
        {
            return new MetadataCallsCount(
                    listSchemasCount,
                    listTablesCount,
                    getTableHandleCount,
                    getColumnsCount,
                    listRoleGrantsCount,
                    rolesPushedCount,
                    granteesPushedCount,
                    limitPushedCount);
        }

        public MetadataCallsCount withLimitPushedCount(long limitPushedCount)
        {
            return new MetadataCallsCount(
                    listSchemasCount,
                    listTablesCount,
                    getTableHandleCount,
                    getColumnsCount,
                    listRoleGrantsCount,
                    rolesPushedCount,
                    granteesPushedCount,
                    limitPushedCount);
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
                    getTableHandleCount == that.getTableHandleCount &&
                    getColumnsCount == that.getColumnsCount &&
                    listRoleGrantsCount == that.listRoleGrantsCount &&
                    rolesPushedCount == that.rolesPushedCount &&
                    granteesPushedCount == that.granteesPushedCount &&
                    limitPushedCount == that.limitPushedCount;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(
                    listSchemasCount,
                    listTablesCount,
                    getTableHandleCount,
                    getColumnsCount,
                    listRoleGrantsCount,
                    rolesPushedCount,
                    granteesPushedCount,
                    limitPushedCount);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("listSchemasCount", listSchemasCount)
                    .add("listTablesCount", listTablesCount)
                    .add("getTableHandleCount", getTableHandleCount)
                    .add("getColumnsCount", getColumnsCount)
                    .add("listRoleGrantsCount", listRoleGrantsCount)
                    .add("rolesPushedCount", rolesPushedCount)
                    .add("granteesPushedCount", granteesPushedCount)
                    .add("limitPushedCount", limitPushedCount)
                    .toString();
        }
    }

    public static class ListRoleGrantsCounter
    {
        private final AtomicLong listRowGrantsCallsCounter = new AtomicLong();
        private final AtomicLong rolesPushedCounter = new AtomicLong();
        private final AtomicLong granteesPushedCounter = new AtomicLong();
        private final AtomicLong limitPushedCounter = new AtomicLong();

        public void reset()
        {
            listRowGrantsCallsCounter.set(0);
            rolesPushedCounter.set(0);
            granteesPushedCounter.set(0);
            limitPushedCounter.set(0);
        }

        public void incrementListRoleGrants(Optional<Set<String>> roles, Optional<Set<String>> grantees, OptionalLong limit)
        {
            listRowGrantsCallsCounter.incrementAndGet();
            roles.ifPresent(x -> rolesPushedCounter.incrementAndGet());
            grantees.ifPresent(x -> granteesPushedCounter.incrementAndGet());
            limit.ifPresent(x -> limitPushedCounter.incrementAndGet());
        }
    }
}
