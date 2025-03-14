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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.metadata.ViewColumn;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.sql.tree.Comment;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.QualifiedName;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.connector.SaveMode.FAIL;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.tree.Comment.Type.COLUMN;
import static io.trino.sql.tree.Comment.Type.TABLE;
import static io.trino.sql.tree.Comment.Type.VIEW;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCommentTask
        extends BaseDataDefinitionTaskTest
{
    @Test
    public void testCommentTable()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);
        assertThat(metadata.getTableMetadata(testSession, metadata.getTableHandle(testSession, tableName).get()).metadata().getComment())
                .isEmpty();

        getFutureValue(setComment(TABLE, asQualifiedName(tableName), Optional.of("new comment")));
        assertThat(metadata.getTableMetadata(testSession, metadata.getTableHandle(testSession, tableName).get()).metadata().getComment())
                .isEqualTo(Optional.of("new comment"));
    }

    @Test
    public void testCommentTableOnView()
    {
        QualifiedObjectName viewName = qualifiedObjectName("existing_view");
        metadata.createView(testSession, viewName, someView(), ImmutableMap.of(), false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(setComment(TABLE, asQualifiedName(viewName), Optional.of("new comment"))))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessageContaining("Table '%1$s' does not exist, but a view with that name exists. Did you mean COMMENT ON VIEW %1$s IS ...?", viewName);
    }

    @Test
    public void testCommentTableOnMaterializedView()
    {
        QualifiedObjectName materializedViewName = qualifiedObjectName("existing_materialized_view");
        metadata.createMaterializedView(testSession, QualifiedObjectName.valueOf(materializedViewName.toString()), someMaterializedView(), MATERIALIZED_VIEW_PROPERTIES, false, false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(setComment(TABLE, asQualifiedName(materializedViewName), Optional.of("new comment"))))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessageContaining("Table '%s' does not exist, but a materialized view with that name exists. Setting comments on materialized views is unsupported.", materializedViewName);
    }

    @Test
    public void testCommentView()
    {
        QualifiedObjectName viewName = qualifiedObjectName("existing_view");
        metadata.createView(testSession, viewName, someView(), ImmutableMap.of(), false);
        assertThat(metadata.isView(testSession, viewName)).isTrue();

        getFutureValue(setComment(VIEW, asQualifiedName(viewName), Optional.of("new comment")));
        assertThat(metadata.getView(testSession, viewName).get().getComment()).isEqualTo(Optional.of("new comment"));
    }

    @Test
    public void testCommentViewOnTable()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);

        assertTrinoExceptionThrownBy(() -> getFutureValue(setComment(VIEW, asQualifiedName(tableName), Optional.of("new comment"))))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessageContaining("View '%1$s' does not exist, but a table with that name exists. Did you mean COMMENT ON TABLE %1$s IS ...?", tableName);
    }

    @Test
    public void testCommentViewOnMaterializedView()
    {
        QualifiedObjectName materializedViewName = qualifiedObjectName("existing_materialized_view");
        metadata.createMaterializedView(testSession, QualifiedObjectName.valueOf(materializedViewName.toString()), someMaterializedView(), MATERIALIZED_VIEW_PROPERTIES, false, false);

        assertTrinoExceptionThrownBy(() -> getFutureValue(setComment(VIEW, asQualifiedName(materializedViewName), Optional.of("new comment"))))
                .hasErrorCode(TABLE_NOT_FOUND)
                .hasMessageContaining("View '%s' does not exist, but a materialized view with that name exists. Setting comments on materialized views is unsupported.", materializedViewName);
    }

    @Test
    public void testCommentTableColumn()
    {
        QualifiedObjectName tableName = qualifiedObjectName("existing_table");
        QualifiedName columnName = qualifiedColumnName("existing_table", "test");
        metadata.createTable(testSession, TEST_CATALOG_NAME, someTable(tableName), FAIL);

        getFutureValue(setComment(COLUMN, columnName, Optional.of("new test column comment")));
        TableHandle tableHandle = metadata.getTableHandle(testSession, tableName).get();
        ConnectorTableMetadata connectorTableMetadata = metadata.getTableMetadata(testSession, tableHandle).metadata();
        assertThat(Optional.ofNullable(connectorTableMetadata.getColumns().stream().filter(column -> "test".equals(column.getName())).collect(onlyElement()).getComment()))
                .isEqualTo(Optional.of("new test column comment"));
    }

    @Test
    public void testCommentViewColumn()
    {
        QualifiedObjectName viewName = qualifiedObjectName("existing_view");
        QualifiedName columnName = qualifiedColumnName("existing_view", "test");
        QualifiedName missingColumnName = qualifiedColumnName("existing_view", "missing");
        metadata.createView(testSession, viewName, someView(), ImmutableMap.of(), false);
        assertThat(metadata.isView(testSession, viewName)).isTrue();

        getFutureValue(setComment(COLUMN, columnName, Optional.of("new test column comment")));
        assertThat(metadata.getView(testSession, viewName).get().getColumns().stream().filter(column -> "test".equals(column.name())).collect(onlyElement()).comment())
                .isEqualTo(Optional.of("new test column comment"));

        assertTrinoExceptionThrownBy(() -> getFutureValue(setComment(COLUMN, missingColumnName, Optional.of("comment for missing column"))))
                .hasErrorCode(COLUMN_NOT_FOUND)
                .hasMessageContaining("Column does not exist: %s", missingColumnName.getSuffix());
    }

    @Test
    public void testCommentOnMixedCaseViewColumn()
    {
        QualifiedObjectName viewName = qualifiedObjectName("existing_view");
        metadata.createView(testSession, viewName, viewDefinition("SELECT 1", ImmutableList.of(new ViewColumn("Mixed", BIGINT.getTypeId(), Optional.empty()))), ImmutableMap.of(), false);
        assertThat(metadata.isView(testSession, viewName)).isTrue();

        QualifiedName columnNameLowerCase = qualifiedColumnName("existing_view", "mixed");
        getFutureValue(setComment(COLUMN, columnNameLowerCase, Optional.of("new mixed column comment")));
        assertThat(metadata.getView(testSession, viewName).get().getColumns().stream().filter(column -> "Mixed".equals(column.name())).collect(onlyElement()).comment())
                .isEqualTo(Optional.of("new mixed column comment"));

        QualifiedName columnNameMixedCase = qualifiedColumnName("existing_view", "Mixed");
        getFutureValue(setComment(COLUMN, columnNameMixedCase, Optional.of("new Mixed column comment")));
        assertThat(metadata.getView(testSession, viewName).get().getColumns().stream().filter(column -> "Mixed".equals(column.name())).collect(onlyElement()).comment())
                .isEqualTo(Optional.of("new Mixed column comment"));
    }

    @Test
    public void testCommentMaterializedViewColumn()
    {
        QualifiedObjectName materializedViewName = qualifiedObjectName("existing_materialized_view");
        metadata.createMaterializedView(testSession, QualifiedObjectName.valueOf(materializedViewName.toString()), someMaterializedView(), MATERIALIZED_VIEW_PROPERTIES, false, false);
        assertThat(metadata.isMaterializedView(testSession, materializedViewName)).isTrue();

        QualifiedName columnName = qualifiedColumnName("existing_materialized_view", "test");
        QualifiedName missingColumnName = qualifiedColumnName("existing_materialized_view", "missing");

        getFutureValue(setComment(COLUMN, columnName, Optional.of("new test column comment")));
        assertThat(metadata.getMaterializedView(testSession, materializedViewName).get().getColumns().stream().filter(column -> "test".equals(column.name())).collect(onlyElement()).comment())
                .isEqualTo(Optional.of("new test column comment"));

        assertTrinoExceptionThrownBy(() -> getFutureValue(setComment(COLUMN, missingColumnName, Optional.of("comment for missing column"))))
                .hasErrorCode(COLUMN_NOT_FOUND)
                .hasMessageContaining("Column does not exist: %s", missingColumnName.getSuffix());
    }

    private ListenableFuture<Void> setComment(Comment.Type type, QualifiedName viewName, Optional<String> comment)
    {
        return new CommentTask(metadata, new AllowAllAccessControl()).execute(new Comment(new NodeLocation(1, 1), type, viewName, comment), queryStateMachine, ImmutableList.of(), WarningCollector.NOOP);
    }
}
