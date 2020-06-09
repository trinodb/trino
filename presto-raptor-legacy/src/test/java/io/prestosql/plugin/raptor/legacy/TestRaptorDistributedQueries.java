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
package io.prestosql.plugin.raptor.legacy;

import com.google.common.collect.ImmutableMap;
import io.prestosql.testing.AbstractTestDistributedQueries;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.sql.TestTable;
import org.testng.SkipException;

import java.util.Optional;

import static io.prestosql.plugin.raptor.legacy.RaptorQueryRunner.createRaptorQueryRunner;

public class TestRaptorDistributedQueries
        extends AbstractTestDistributedQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createRaptorQueryRunner(ImmutableMap.of(), true, false);
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        throw new SkipException("Raptor connector does not support column default values");
    }

    @Override
    public void testCommentTable()
    {
        // Raptor connector currently does not support comment on table
        assertQueryFails("COMMENT ON TABLE orders IS 'hello'", "This connector does not support setting table comments");
    }

    @Override
    public void testInsertWithCoercion()
    {
        // No support for char type
    }

    @Override
    public void testCreateSchema()
    {
        // schema creation is not supported
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getPrestoTypeName();
        if (typeName.equals("tinyint")
                || typeName.equals("real")
                || typeName.startsWith("decimal(")
                || typeName.equals("time")
                || typeName.equals("timestamp(3) with time zone")
                || typeName.startsWith("char(")) {
            // TODO this should either work or fail cleanly
            return Optional.empty();
        }

        return Optional.of(dataMappingTestSetup);
    }
}
