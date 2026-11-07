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
package io.trino.connector.system;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonModule;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.metadata.HandleJsonModule;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemColumnHandle;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.TestingConnectorSession;
import io.trino.transaction.TestingTransactionManager;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static io.trino.metadata.TestingMetadataManager.createTestingMetadataManager;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestSystemTablesMetadata
{
    private static final SchemaTableName TABLE_NAME = new SchemaTableName("test", "files");

    @Test
    void testSourceTableHandlesAreCapturedAndPreservedAfterFiltering()
    {
        MockSystemTable systemTable = new MockSystemTable();
        ConnectorMetadata metadata = createMetadata(systemTable);

        SystemTableHandle capturedHandle = (SystemTableHandle) metadata.getTableHandle(
                TestingConnectorSession.SESSION,
                TABLE_NAME,
                Optional.empty(),
                Optional.empty());
        MockConnectorTableHandle originalSource = (MockConnectorTableHandle) systemTable.sourceTableHandles.getFirst();
        MockConnectorTableHandle laterSource = new MockConnectorTableHandle(new SchemaTableName("source", "later"));
        systemTable.sourceTableHandles.set(0, laterSource);

        assertThat(capturedHandle.sourceTableHandles()).containsExactly(originalSource);
        assertThatThrownBy(() -> capturedHandle.sourceTableHandles().add(laterSource))
                .isInstanceOf(UnsupportedOperationException.class);

        SystemTableHandle filteredHandle = (SystemTableHandle) metadata.applyFilter(
                        TestingConnectorSession.SESSION,
                        capturedHandle,
                        new Constraint(TupleDomain.withColumnDomains(ImmutableMap.of(
                                new SystemColumnHandle("value"), Domain.singleValue(BIGINT, 1L)))))
                .orElseThrow()
                .getHandle();
        assertThat(filteredHandle.sourceTableHandles()).containsExactly(originalSource);

        SystemTableHandle newlyResolvedHandle = (SystemTableHandle) metadata.getTableHandle(
                TestingConnectorSession.SESSION,
                TABLE_NAME,
                Optional.empty(),
                Optional.empty());
        assertThat(newlyResolvedHandle.sourceTableHandles()).containsExactly(laterSource);
    }

    @Test
    void testSourceTableHandlesSerialization()
    {
        JsonCodec<SystemTableHandle> jsonCodec = createJsonCodec();
        SystemTableHandle expected = new SystemTableHandle(
                TABLE_NAME.getSchemaName(),
                TABLE_NAME.getTableName(),
                TupleDomain.all(),
                List.of(new MockConnectorTableHandle(new SchemaTableName("source", "captured"))));

        assertThat(jsonCodec.fromJson(jsonCodec.toJson(expected))).isEqualTo(expected);
    }

    private static ConnectorMetadata createMetadata(SystemTable systemTable)
    {
        SystemTablesProvider tables = new SystemTablesProvider(
                new TestingTransactionManager(),
                createTestingMetadataManager(),
                "test",
                Set.of(systemTable));
        return new SystemTablesMetadata(tables);
    }

    private static JsonCodec<SystemTableHandle> createJsonCodec()
    {
        Injector injector = new Bootstrap(
                new JsonModule(),
                new HandleJsonModule(),
                binder -> jsonCodecBinder(binder).bindJsonCodec(SystemTableHandle.class))
                .doNotInitializeLogging()
                .initialize();
        return injector.getInstance(Key.get(new TypeLiteral<>() {}));
    }

    private static final class MockSystemTable
            implements SystemTable
    {
        private final List<ConnectorTableHandle> sourceTableHandles = new ArrayList<>(List.of(
                new MockConnectorTableHandle(new SchemaTableName("source", "captured"))));

        @Override
        public Distribution getDistribution()
        {
            return Distribution.SINGLE_COORDINATOR;
        }

        @Override
        public ConnectorTableMetadata getTableMetadata()
        {
            return new ConnectorTableMetadata(TABLE_NAME, List.of(new ColumnMetadata("value", BIGINT)));
        }

        @Override
        public List<ConnectorTableHandle> getSourceTableHandles()
        {
            return sourceTableHandles;
        }
    }
}
