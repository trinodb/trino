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
package io.trino.tests.product.warp.utils.syntheticconfig;

import io.trino.tests.product.warp.utils.TestFormat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.trino.tests.product.warp.utils.syntheticconfig.TestConfiguration.QUERY_ID;
import static io.trino.tests.product.warp.utils.syntheticconfig.TestConfiguration.TEST_NAME;

public class IncludeStrategy
        implements TestParser
{
    @Override
    public List<TestFormat> parse(Map<String, List<String>> configuration, List<TestFormat> tests)
    {
        List<String> queriesToInclude = configuration.getOrDefault(QUERY_ID, new ArrayList<>());
        List<String> testsToRun = new ArrayList<>(configuration.getOrDefault(TEST_NAME, new ArrayList<>()));
        if (!queriesToInclude.isEmpty()) {
            for (TestFormat test : tests) {
                List<TestFormat.QueryData> queries = new ArrayList<>();
                for (TestFormat.QueryData queryData : test.queries_data()) {
                    if (queriesToInclude.contains(queryData.query_id())) {
                        configuration.computeIfAbsent(TEST_NAME, k -> new ArrayList<>()).add(test.name());
                    }
                    else if (!testsToRun.contains(test.name())) {
                        queries.add(queryData);
                    }
                }
                test.queries_data().removeAll(queries);
            }
        }

        List<String> testNamesToRun = new ArrayList<>(configuration.getOrDefault(TEST_NAME, new ArrayList<>()));
        return tests.stream().filter(test -> testNamesToRun.contains(test.name())).toList();
    }
}
