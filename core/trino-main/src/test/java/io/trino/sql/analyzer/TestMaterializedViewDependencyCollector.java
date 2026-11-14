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
package io.trino.sql.analyzer;

import io.trino.connector.MockConnectorTableHandle;
import io.trino.metadata.MaterializedViewDependency;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.metadata.ViewHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.BasicViewHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.TestingTransactionHandle;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestMaterializedViewDependencyCollector
{
    private static final QualifiedObjectName MATERIALIZED_VIEW = name("mv");
    private static final QualifiedObjectName VIEW = name("v");
    private static final QualifiedObjectName TABLE = name("t");

    // Handles have no value equality, so each name must always map to the same instance
    private static final Map<QualifiedObjectName, ViewHandle> VIEW_HANDLES = new ConcurrentHashMap<>();
    private static final Map<QualifiedObjectName, TableHandle> TABLE_HANDLES = new ConcurrentHashMap<>();

    @Test
    public void testExpandMaterializedView()
    {
        MaterializedViewDependencyCollector collector = new MaterializedViewDependencyCollector();
        Optional<String> result = collector.expandMaterializedView(MATERIALIZED_VIEW, viewHandle(MATERIALIZED_VIEW), Optional.of(tableHandle(name("storage"))), () -> {
            collector.beginView(VIEW, viewHandle(VIEW));
            collector.addTable(TABLE, tableHandle(TABLE));
            collector.endView();
            return "expanded";
        });

        assertThat(result).contains("expanded");
        assertThat(collector.build()).containsExactly(
                new MaterializedViewDependency.MaterializedView(
                        MATERIALIZED_VIEW,
                        viewHandle(MATERIALIZED_VIEW),
                        Optional.of(tableHandle(name("storage"))),
                        Optional.of(List.of(new MaterializedViewDependency.View(
                                VIEW,
                                viewHandle(VIEW),
                                List.of(new MaterializedViewDependency.Table(TABLE, tableHandle(TABLE))))))));
    }

    @Test
    public void testFailedExpansionWithoutStorageTable()
    {
        MaterializedViewDependencyCollector collector = new MaterializedViewDependencyCollector();
        assertThatThrownBy(() -> collector.expandMaterializedView(MATERIALIZED_VIEW, viewHandle(MATERIALIZED_VIEW), Optional.empty(), () -> {
            throw new TrinoException(TABLE_NOT_FOUND, "table was dropped");
        }))
                .isInstanceOf(TrinoException.class)
                .hasMessage("table was dropped");
    }

    @Test
    public void testFailedExpansionWithStorageTable()
    {
        MaterializedViewDependencyCollector collector = new MaterializedViewDependencyCollector();
        Optional<String> result = collector.expandMaterializedView(MATERIALIZED_VIEW, viewHandle(MATERIALIZED_VIEW), Optional.of(tableHandle(name("storage"))), () -> {
            collector.addTable(TABLE, tableHandle(TABLE));
            // The analysis fails while the view is still being expanded
            collector.beginView(VIEW, viewHandle(VIEW));
            throw new TrinoException(TABLE_NOT_FOUND, "table was dropped");
        });

        assertThat(result).isEmpty();
        // build() fails if the view left open by the failed analysis was not discarded
        assertThat(collector.build()).containsExactly(
                new MaterializedViewDependency.MaterializedView(
                        MATERIALIZED_VIEW,
                        viewHandle(MATERIALIZED_VIEW),
                        Optional.of(tableHandle(name("storage"))),
                        Optional.empty()));
    }

    @Test
    public void testFailedNestedExpansionKeepsOuterDependencies()
    {
        MaterializedViewDependencyCollector collector = new MaterializedViewDependencyCollector();
        collector.expandMaterializedView(MATERIALIZED_VIEW, viewHandle(MATERIALIZED_VIEW), Optional.of(tableHandle(name("storage"))), () -> {
            collector.addTable(TABLE, tableHandle(TABLE));
            Optional<String> nested = collector.expandMaterializedView(name("inner"), viewHandle(name("inner")), Optional.of(tableHandle(name("inner_storage"))), () -> {
                throw new TrinoException(TABLE_NOT_FOUND, "table was dropped");
            });
            assertThat(nested).isEmpty();
            return "expanded";
        });

        assertThat(collector.build()).containsExactly(
                new MaterializedViewDependency.MaterializedView(
                        MATERIALIZED_VIEW,
                        viewHandle(MATERIALIZED_VIEW),
                        Optional.of(tableHandle(name("storage"))),
                        Optional.of(List.of(
                                new MaterializedViewDependency.Table(TABLE, tableHandle(TABLE)),
                                new MaterializedViewDependency.MaterializedView(
                                        name("inner"),
                                        viewHandle(name("inner")),
                                        Optional.of(tableHandle(name("inner_storage"))),
                                        Optional.empty())))));
    }

    private static QualifiedObjectName name(String objectName)
    {
        return new QualifiedObjectName("test_catalog", "test_schema", objectName);
    }

    private static ViewHandle viewHandle(QualifiedObjectName name)
    {
        return VIEW_HANDLES.computeIfAbsent(name, key -> new ViewHandle(TEST_CATALOG_HANDLE, new BasicViewHandle(new SchemaTableName(key.schemaName(), key.objectName()))));
    }

    private static TableHandle tableHandle(QualifiedObjectName name)
    {
        return TABLE_HANDLES.computeIfAbsent(name, key -> new TableHandle(TEST_CATALOG_HANDLE, new MockConnectorTableHandle(new SchemaTableName(key.schemaName(), key.objectName())), TestingTransactionHandle.create()));
    }
}
