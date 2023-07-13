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
package io.trino.tests.product;

import io.trino.tempto.ProductTest;
import io.trino.tempto.Requires;
import io.trino.tempto.fulfillment.table.hive.tpch.ImmutableTpchTablesRequirements.ImmutableNationTable;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.context.ContextDsl.executeWith;
import static io.trino.tempto.sql.SqlContexts.createViewAs;
import static io.trino.tests.product.TestGroups.CREATE_DROP_VIEW;
import static io.trino.tests.product.TestGroups.SMOKE;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

@Requires(ImmutableNationTable.class)
public class TestCreateDropView
        extends ProductTest
{
    @Test(groups = CREATE_DROP_VIEW)
    public void createSimpleView()
    {
        executeWith(createViewAs("SELECT * FROM nation", onTrino()), view -> {
            assertThat(onTrino().executeQuery(format("SELECT * FROM %s", view.getName())))
                    .hasRowsCount(25);
        });
    }

    @Test(groups = CREATE_DROP_VIEW)
    public void querySimpleViewQualified()
    {
        executeWith(createViewAs("SELECT * FROM nation", onTrino()), view -> {
            assertThat(onTrino().executeQuery(format("SELECT %s.n_regionkey FROM %s", view.getName(), view.getName())))
                    .hasRowsCount(25);
        });
    }

    @Test(groups = CREATE_DROP_VIEW)
    public void createViewWithAggregate()
    {
        executeWith(createViewAs("SELECT n_regionkey, count(*) countries FROM nation GROUP BY n_regionkey ORDER BY n_regionkey", onTrino()), view -> {
            assertThat(onTrino().executeQuery(format("SELECT * FROM %s", view.getName())))
                    .hasRowsCount(5);
        });
    }

    @Test(groups = {CREATE_DROP_VIEW, SMOKE})
    public void createOrReplaceSimpleView()
    {
        executeWith(createViewAs("SELECT * FROM nation", onTrino()), view -> {
            assertThat(onTrino().executeQuery(format("CREATE OR REPLACE VIEW %s AS SELECT * FROM nation", view.getName())))
                    .hasRowsCount(1);
            assertThat(onTrino().executeQuery(format("SELECT * FROM %s", view.getName())))
                    .hasRowsCount(25);
        });
    }

    @Test(groups = CREATE_DROP_VIEW)
    public void createSimpleViewTwiceShouldFail()
    {
        executeWith(createViewAs("SELECT * FROM nation", onTrino()), view -> {
            assertQueryFailure(() -> onTrino().executeQuery(format("CREATE VIEW %s AS SELECT * FROM nation", view.getName())))
                    .hasMessageContaining("View already exists");
            assertThat(onTrino().executeQuery(format("SELECT * FROM %s", view.getName())))
                    .hasRowsCount(25);
        });
    }

    @Test(groups = {CREATE_DROP_VIEW, SMOKE})
    public void dropViewTest()
    {
        executeWith(createViewAs("SELECT * FROM nation", onTrino()), view -> {
            assertThat(onTrino().executeQuery(format("SELECT * FROM %s", view.getName())))
                    .hasRowsCount(25);
            assertThat(onTrino().executeQuery(format("DROP VIEW %s", view.getName())))
                    .hasRowsCount(1);
            assertQueryFailure(() -> onTrino().executeQuery(format("SELECT * FROM %s", view.getName())))
                    .hasMessageContaining("does not exist");
        });
    }
}
