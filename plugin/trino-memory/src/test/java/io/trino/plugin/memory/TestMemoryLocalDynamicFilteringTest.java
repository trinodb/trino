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
package io.trino.plugin.memory;

import com.google.common.collect.ImmutableList;
import io.trino.testing.AbstractLocalDynamicFilteringTest;
import io.trino.testing.QueryRunner;

import static io.trino.plugin.memory.MemoryQueryRunner.createMemoryQueryRunner;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.PART;

public class TestMemoryLocalDynamicFilteringTest
        extends AbstractLocalDynamicFilteringTest
{
    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        return createMemoryQueryRunner(
                getPropertyMap(),
                ImmutableList.of(NATION, CUSTOMER, ORDERS, LINE_ITEM, PART));
    }
}
