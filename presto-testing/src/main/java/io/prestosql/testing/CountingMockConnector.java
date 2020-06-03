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
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.RoleGrant;

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
import static io.prestosql.connector.MockConnectorFactory.Builder.defaultGetColumns;
import static io.prestosql.spi.security.PrincipalType.USER;

public class CountingMockConnector
{
    private final Object lock = new Object();

    private final List<SchemaTableName> tablesTestSchema1 = IntStream.range(0, 1000)
            .mapToObj(i -> new SchemaTableName("test_schema1", "test_table" + i))
            .collect(toImmutableList());

    private final List<SchemaTableName> tablesTestSchema2 = IntStream.range(0, 2000)
            .mapToObj(i -> new SchemaTableName("test_schema2", "test_table" + i))
            .collect(toImmutableList());

    private final Set<RoleGrant> roleGrants = IntStream.range(0, 100)
            .mapToObj(i -> new RoleGrant(new PrestoPrincipal(USER, "user" + (i == 0 ? "" : i)), "role" + i / 2, false))
            .collect(toImmutableSet());

    private final AtomicLong listSchemasCallsCounter = new AtomicLong();
    private final AtomicLong listTablesCallsCounter = new AtomicLong();
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
                tablesTestSchema1.stream(),
                tablesTestSchema2.stream());
    }

    public MetadataCallsCount runCounting(Runnable runnable)
    {
        synchronized (lock) {
            listSchemasCallsCounter.set(0);
            listTablesCallsCounter.set(0);
            getColumnsCallsCounter.set(0);
            listRoleGranstCounter.reset();

            runnable.run();

            return new MetadataCallsCount(listSchemasCallsCounter.get(),
                    listTablesCallsCounter.get(),
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
        private final long getColumnsCount;
        private final long listRoleGrantsCount;
        private final long rolesPushedCount;
        private final long granteesPushedCount;
        private final long limitPushedCount;

        public MetadataCallsCount()
        {
            this(0, 0, 0, 0, 0, 0, 0);
        }

        public MetadataCallsCount(long listSchemasCount,
                long listTablesCount,
                long getColumnsCount,
                long listRoleGrantsCount,
                long rolesPushedCount,
                long granteesPushedCount,
                long limitPushedCount)
        {
            this.listSchemasCount = listSchemasCount;
            this.listTablesCount = listTablesCount;
            this.getColumnsCount = getColumnsCount;
            this.listRoleGrantsCount = listRoleGrantsCount;
            this.rolesPushedCount = rolesPushedCount;
            this.granteesPushedCount = granteesPushedCount;
            this.limitPushedCount = limitPushedCount;
        }

        public MetadataCallsCount withListSchemasCount(long listSchemasCount)
        {
            return new MetadataCallsCount(listSchemasCount, listTablesCount, getColumnsCount, listRoleGrantsCount, rolesPushedCount, granteesPushedCount, limitPushedCount);
        }

        public MetadataCallsCount withListTablesCount(long listTablesCount)
        {
            return new MetadataCallsCount(listSchemasCount, listTablesCount, getColumnsCount, listRoleGrantsCount, rolesPushedCount, granteesPushedCount, limitPushedCount);
        }

        public MetadataCallsCount withGetColumnsCount(long getColumnsCount)
        {
            return new MetadataCallsCount(listSchemasCount, listTablesCount, getColumnsCount, listRoleGrantsCount, rolesPushedCount, granteesPushedCount, limitPushedCount);
        }

        public MetadataCallsCount withListRoleGrantsCount(long listRoleGrantsCount)
        {
            return new MetadataCallsCount(listSchemasCount, listTablesCount, getColumnsCount, listRoleGrantsCount, rolesPushedCount, granteesPushedCount, limitPushedCount);
        }

        public MetadataCallsCount withRolesPushedCount(long rolesPushedCount)
        {
            return new MetadataCallsCount(listSchemasCount, listTablesCount, getColumnsCount, listRoleGrantsCount, rolesPushedCount, granteesPushedCount, limitPushedCount);
        }

        public MetadataCallsCount withGranteesPushedCount(long granteesPushedCount)
        {
            return new MetadataCallsCount(listSchemasCount, listTablesCount, getColumnsCount, listRoleGrantsCount, rolesPushedCount, granteesPushedCount, limitPushedCount);
        }

        public MetadataCallsCount withLimitPushedCount(long limitPushedCount)
        {
            return new MetadataCallsCount(listSchemasCount, listTablesCount, getColumnsCount, listRoleGrantsCount, rolesPushedCount, granteesPushedCount, limitPushedCount);
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
                    getColumnsCount == that.getColumnsCount &&
                    listRoleGrantsCount == that.listRoleGrantsCount &&
                    rolesPushedCount == that.rolesPushedCount &&
                    granteesPushedCount == that.granteesPushedCount &&
                    limitPushedCount == that.limitPushedCount;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(listSchemasCount,
                    listTablesCount,
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
