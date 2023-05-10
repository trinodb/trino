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
package io.trino.tests.tpch;

import io.trino.testing.BaseCacheSubqueriesTest;
import io.trino.testing.QueryRunner;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.util.List;

@Test(singleThreaded = true)
public class TestTpchCacheSubqueriesTest
        extends BaseCacheSubqueriesTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TpchQueryRunnerBuilder.builder()
                .addExtraProperties(EXTRA_PROPERTIES)
                // cache doesn't support table partitioning yet
                .withPartitioningEnabled(false)
                // create enough splits for caching to be effective
                .withSplitsPerNode(100)
                .build();
    }

    @Override
    public void testCacheWhenProjectionsWerePushedDown()
    {
        throw new SkipException("tpch does not support for pushing down projections");
    }

    @Override
    @Test(dataProvider = "isDynamicRowFilteringEnabled")
    public void testDynamicFilterCache(boolean isDynamicRowFilteringEnabled)
    {
        throw new SkipException("tpch does not support for partitioned tables");
    }

    @Override
    @Test
    public void testPredicateOnPartitioningColumnThatWasNotFullyPushed()
    {
        throw new SkipException("tpch does not support for partitioned tables");
    }

    @Override
    @Test
    public void testPartitionedQueryCache()
    {
        throw new SkipException("tpch does not support for partitioned tables");
    }

    @Override
    @Test(dataProvider = "isDynamicRowFilteringEnabled")
    public void testSimplifyAndPrunePredicate(boolean isDynamicRowFilteringEnabled)
    {
        throw new SkipException("tpch does not support for partitioned tables");
    }

    @Override
    protected void createPartitionedTableAsSelect(String tableName, List<String> partitionColumns, String asSelect)
    {
        throw new UnsupportedOperationException("tpch does not support for partitioned tables");
    }
}
