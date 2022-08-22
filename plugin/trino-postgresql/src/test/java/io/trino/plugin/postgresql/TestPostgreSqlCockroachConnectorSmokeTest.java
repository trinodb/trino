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
package io.trino.plugin.postgresql;

import io.trino.plugin.jdbc.BaseJdbcConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;

import java.util.Map;

import static io.trino.plugin.postgresql.PostgreSqlQueryRunner.createPostgreSqlQueryRunner;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// TODO https://github.com/trinodb/trino/issues/13771 Move to CockroachDB connector
public class TestPostgreSqlCockroachConnectorSmokeTest
        extends BaseJdbcConnectorSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingCockroachServer cockroachServer = closeAfterClass(new TestingCockroachServer());
        return createPostgreSqlQueryRunner(
                cockroachServer,
                Map.of(),
                Map.of(),
                REQUIRED_TPCH_TABLES);
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS:
                return false;
            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    public void testSelectInformationSchemaColumns()
    {
        // Override because CockroachDB has a 'rowid' column
        assertThat(query(format("SELECT column_name FROM information_schema.columns WHERE table_schema = '%s' AND table_name = 'region'", getSession().getSchema().orElseThrow())))
                .skippingTypesCheck()
                .matches("VALUES 'regionkey', 'name', 'comment', 'rowid'");
    }

    @Override
    public void testShowCreateTable()
    {
        // Override because CockroachDB has a 'rowid' column
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .isEqualTo("" +
                        "CREATE TABLE postgresql.tpch.region (\n" +
                        "   regionkey bigint,\n" +
                        "   name varchar(25),\n" +
                        "   comment varchar(152),\n" +
                        "   rowid bigint NOT NULL\n" +
                        ")");
    }

    @Override
    public void testTopN()
    {
        // Override because CockroachDB doesn't support 'NULLS FIRST|LAST LIMIT' syntax
        assertThatThrownBy(() -> super.testTopN())
                .hasStackTraceContaining("You have attempted to use a feature that is not yet implemented.");
    }
}
