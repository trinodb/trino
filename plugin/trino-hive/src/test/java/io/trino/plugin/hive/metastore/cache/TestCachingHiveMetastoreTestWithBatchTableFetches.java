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
package io.trino.plugin.hive.metastore.cache;

import org.testng.annotations.Test;

@Test(singleThreaded = true)
public class TestCachingHiveMetastoreTestWithBatchTableFetches
        extends BaseCachingHiveMetastoreTest
{
    public TestCachingHiveMetastoreTestWithBatchTableFetches()
    {
        super(true);
    }

    @Override
    @Test(enabled = false)
    public void testCachingHiveMetastoreCreationViaMemoize()
    {
        // This test will fail as creating bridging metastore pollutes accessCount stats
    }
}
