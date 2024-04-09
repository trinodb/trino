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
package io.trino.plugin.opensearch;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.parallel.Isolated;

import java.util.List;

@Isolated
public class TestOpenSearchLatestConnectorTest
        extends BaseOpenSearchConnectorTest
{
    public TestOpenSearchLatestConnectorTest()
    {
        super("opensearchproject/opensearch:latest", "opensearch");
    }

    @Override
    protected List<Integer> largeInValuesCountData()
    {
        // 1000 IN fails with "Query contains too many nested clauses; maxClauseCount is set to 1024"
        return ImmutableList.of(200, 500);
    }
}
