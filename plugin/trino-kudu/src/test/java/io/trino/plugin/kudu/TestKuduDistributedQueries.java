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
package io.trino.plugin.kudu;

import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import java.util.Optional;

import static io.trino.plugin.kudu.KuduQueryRunnerFactory.createKuduQueryRunnerTpch;

public class TestKuduDistributedQueries
        extends BaseKuduDistributedQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingKuduServer kuduServer = closeAfterClass(new TestingKuduServer());
        return createKuduQueryRunnerTpch(kuduServer, Optional.of(""), TpchTable.getTables());
    }
}
