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
package io.prestosql.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.prestosql.tests.AbstractTestDistributedQueries;

import static io.prestosql.plugin.iceberg.IcebergQueryRunner.createIcebergQueryRunner;

public class TestIcebergDistributed
        extends AbstractTestDistributedQueries
{
    public TestIcebergDistributed()
    {
        super(() -> createIcebergQueryRunner(ImmutableMap.of()));
    }

    @Override
    protected boolean supportsViews()
    {
        return false;
    }

    @Override
    public void testDelete()
    {
        // Neither row delete nor partition delete is supported yet
    }

    @Override
    public void testDescribeOutput()
    {
        // Iceberg does not support parameterized varchar
    }

    @Override
    public void testDescribeOutputNamedAndUnnamed()
    {
        // Iceberg does not support parameterized varchar
    }

    @Override
    public void testCommentTable()
    {
        // Iceberg connector does not support comment on table
        assertQueryFails("COMMENT ON TABLE orders IS 'hello'", "This connector does not support setting table comments");
    }

    @Override
    public void testRenameTable()
    {
        assertQueryFails("ALTER TABLE orders RENAME TO rename_orders", "Rename not supported for Iceberg tables");
    }
}
