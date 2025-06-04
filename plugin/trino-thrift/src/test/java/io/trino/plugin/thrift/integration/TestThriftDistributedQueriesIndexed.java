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
package io.trino.plugin.thrift.integration;

import io.trino.testing.AbstractTestIndexedQueries;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.thrift.integration.ThriftQueryRunner.startThriftServers;

public class TestThriftDistributedQueriesIndexed
        extends AbstractTestIndexedQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        ThriftQueryRunner.StartedServers servers = startThriftServers(2, true);
        servers.resources().forEach(this::closeAfterClass);
        return ThriftQueryRunner.builder(servers).build();
    }

    @Test
    @Override
    public void testExampleSystemTable()
    {
        // system tables are not supported
    }
}
