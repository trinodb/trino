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
package io.trino.plugin.jdbc;

import io.trino.operator.RetryPolicy;
import io.trino.testing.BaseFailureRecoveryTest;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.connector.ConnectorMetadata.MODIFYING_ROWS_MESSAGE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;

public abstract class BaseJdbcFailureRecoveryTest
        extends BaseFailureRecoveryTest
{
    public BaseJdbcFailureRecoveryTest(RetryPolicy retryPolicy)
    {
        super(retryPolicy);
    }

    @Test
    @Override
    protected void testAnalyzeTable()
    {
        assertThatThrownBy(super::testAnalyzeTable).hasMessageMatching("This connector does not support analyze");
        abort("skipped");
    }

    @Test
    @Override
    protected void testDelete()
    {
        // This simple delete on JDBC ends up as a very simple, single-fragment, coordinator-only plan,
        // which has no ability to recover from errors. This test simply verifies that's still the case.
        Optional<String> setupQuery = Optional.of("CREATE TABLE <table> AS SELECT * FROM orders");
        String testQuery = "DELETE FROM <table> WHERE orderkey = 1";
        Optional<String> cleanupQuery = Optional.of("DROP TABLE <table>");

        assertThatQuery(testQuery)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .isCoordinatorOnly();
    }

    @Test
    @Override
    protected void testDeleteWithSubquery()
    {
        if (supportsMerge()) {
            super.testDeleteWithSubquery();
            return;
        }

        assertThatThrownBy(super::testDeleteWithSubquery).hasMessageContaining(MODIFYING_ROWS_MESSAGE);
    }

    @Test
    @Override
    protected void testRefreshMaterializedView()
    {
        assertThatThrownBy(super::testRefreshMaterializedView)
                .hasMessageContaining("This connector does not support creating materialized views");
        abort("skipped");
    }

    @Test
    @Override
    protected void testUpdate()
    {
        assertThatThrownBy(super::testUpdate).hasMessageContaining(MODIFYING_ROWS_MESSAGE);
        abort("skipped");
    }

    @Test
    @Override
    protected void testUpdateWithSubquery()
    {
        if (supportsMerge()) {
            super.testUpdateWithSubquery();
            return;
        }

        assertThatThrownBy(super::testUpdateWithSubquery).hasMessageContaining(MODIFYING_ROWS_MESSAGE);
    }

    @Test
    @Override
    protected void testMerge()
    {
        if (supportsMerge()) {
            super.testMerge();
            return;
        }

        assertThatThrownBy(super::testMerge).hasMessageContaining(MODIFYING_ROWS_MESSAGE);
    }

    @Override
    protected boolean areWriteRetriesSupported()
    {
        return true;
    }

    protected boolean supportsMerge()
    {
        return false;
    }
}
