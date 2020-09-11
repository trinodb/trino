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
package io.prestosql.plugin.mongodb;

import com.google.common.collect.ImmutableMap;
import io.prestosql.testing.AbstractTestDistributedQueries;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.sql.TestTable;
import io.prestosql.tpch.TpchTable;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.plugin.mongodb.MongoQueryRunner.createMongoQueryRunner;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Test
public class TestMongoDistributedQueries
        extends AbstractTestDistributedQueries
{
    private MongoServer server;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.server = new MongoServer();
        return createMongoQueryRunner(server, ImmutableMap.of(), TpchTable.getTables());
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        server.close();
    }

    @Override
    protected boolean supportsViews()
    {
        return false;
    }

    @Override
    public void testCreateSchema()
    {
        // the connector does not support creating schemas
    }

    @Override
    public void testRenameTable()
    {
        // the connector does not support renaming tables
    }

    @Override
    public void testAddColumn()
    {
        // the connector does not support adding columns
    }

    @Override
    public void testRenameColumn()
    {
        // the connector does not support renaming columns
    }

    @Override
    public void testDropColumn()
    {
        // the connector does not support dropping columns
    }

    @Override
    public void testDelete()
    {
        // the connector does not support delete
    }

    @Override
    public void testCommentTable()
    {
        // the connector does not support comment on table
        assertQueryFails("COMMENT ON TABLE orders IS 'hello'", "This connector does not support setting table comments");
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        throw new SkipException("test disabled for Mongo");
    }

    @Override
    @Test(dataProvider = "testColumnNameDataProvider")
    public void testColumnName(String columnName)
    {
        if (columnName.equals("a.dot")) {
            // TODO (https://github.com/prestosql/presto/issues/3460)
            assertThatThrownBy(() -> super.testColumnName(columnName))
                    .hasStackTraceContaining("TableWriterOperator") // during INSERT
                    .hasMessage("Invalid BSON field name a.dot");
            throw new SkipException("Insert would fail");
        }

        super.testColumnName(columnName);
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getPrestoTypeName();
        if (typeName.equals("time")) {
            // TODO this should either work or fail cleanly
            return Optional.empty();
        }

        return Optional.of(dataMappingTestSetup);
    }
}
