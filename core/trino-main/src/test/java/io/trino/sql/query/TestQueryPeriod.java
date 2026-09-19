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
package io.trino.sql.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.PointerType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.connector.PointerType.TARGET_ID;
import static io.trino.spi.connector.PointerType.TEMPORAL;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static io.trino.testing.TestingAccessControlManager.branchPrivilege;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestQueryPeriod
{
    private final QueryAssertions assertions;

    public TestQueryPeriod()
    {
        QueryRunner runner = new StandaloneQueryRunner(testSessionBuilder()
                .setCatalog("local")
                .setSchema("default")
                .build());
        runner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withGetColumns(_ -> ImmutableList.of(new ColumnMetadata("value", BIGINT)))
                .withData(_ -> ImmutableList.of(ImmutableList.of(42L)))
                .withMetadataWrapper(RangeMetadata::new)
                .build()));
        runner.createCatalog("local", "mock", ImmutableMap.of());
        // A string range endpoint must not be interpreted as a branch for authorization.
        runner.getAccessControl().deny(branchPrivilege("versions", "v2", SELECT_COLUMN));
        assertions = new QueryAssertions(runner);
    }

    @AfterAll
    public void close()
    {
        assertions.close();
    }

    @Test
    public void testVersionRange()
    {
        assertThat(assertions.query("SELECT * FROM versions FOR VERSION FROM 'v1' TO 'v2'"))
                .matches("VALUES BIGINT '42'");
        assertThat(assertions.query("SELECT * FROM versions FOR VERSION FROM CAST('v' || '1' AS varchar(2)) TO CAST('v' || '2' AS varchar(2)) AS t WHERE value = 42"))
                .matches("VALUES BIGINT '42'");
    }

    @Test
    public void testParameterizedRange()
    {
        assertThat(assertions.query("EXECUTE IMMEDIATE 'SELECT * FROM versions FOR VERSION FROM ? TO ?' USING 'v1', 'v2'"))
                .matches("VALUES BIGINT '42'");
    }

    @Test
    public void testTimestampRange()
    {
        assertThat(assertions.query("SELECT * FROM timestamps FOR TIMESTAMP FROM TIMESTAMP '2021-01-01 00:00:00' TO TIMESTAMP '2021-01-02 00:00:00'"))
                .matches("VALUES BIGINT '42'");
    }

    @Test
    public void testAsOf()
    {
        assertThat(assertions.query("SELECT * FROM snapshot FOR VERSION AS OF 'v2'"))
                .matches("VALUES BIGINT '42'");
    }

    @Test
    public void testInvalidBounds()
    {
        assertThat(assertions.query("SELECT * FROM versions FOR VERSION FROM NULL TO 'v2'"))
                .failure().hasMessageContaining("Pointer value cannot be NULL");
        assertThat(assertions.query("SELECT * FROM versions FOR VERSION FROM 'v1' TO NULL"))
                .failure().hasMessageContaining("Pointer value cannot be NULL");
        assertThat(assertions.query("SELECT * FROM timestamps FOR TIMESTAMP FROM '2021-01-01' TO TIMESTAMP '2021-01-02 00:00:00'"))
                .failure().hasMessageContaining("must be of type");
    }

    private static class RangeMetadata
            implements ConnectorMetadata
    {
        private final ConnectorMetadata delegate;

        public RangeMetadata(ConnectorMetadata delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
        {
            switch (tableName.getTableName()) {
                case "versions" -> {
                    assertVersion(startVersion, TARGET_ID, createVarcharType(2), utf8Slice("v1"));
                    assertVersion(endVersion, TARGET_ID, createVarcharType(2), utf8Slice("v2"));
                }
                case "timestamps" -> {
                    assertVersion(startVersion, TEMPORAL, TIMESTAMP_SECONDS, 1_609_459_200_000_000L);
                    assertVersion(endVersion, TEMPORAL, TIMESTAMP_SECONDS, 1_609_545_600_000_000L);
                }
                case "snapshot" -> {
                    assertThat(startVersion).isEmpty();
                    assertVersion(endVersion, TARGET_ID, createVarcharType(2), utf8Slice("v2"));
                }
                default -> throw new IllegalArgumentException("Unexpected table: " + tableName);
            }
            return delegate.getTableHandle(session, tableName, Optional.empty(), Optional.empty());
        }

        private static void assertVersion(Optional<ConnectorTableVersion> version, PointerType pointerType, Type type, Object value)
        {
            assertThat(version).isPresent();
            assertThat(version.orElseThrow().getPointerType()).isEqualTo(pointerType);
            assertThat(version.orElseThrow().getVersionType()).isEqualTo(type);
            assertThat(version.orElseThrow().getVersion()).isEqualTo(value);
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
        {
            return delegate.getTableMetadata(session, table);
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table)
        {
            return delegate.getColumnHandles(session, table);
        }

        @Override
        public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle table, ColumnHandle column)
        {
            return delegate.getColumnMetadata(session, table, column);
        }
    }
}
