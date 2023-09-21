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
package io.trino.sql.query;

import org.junit.jupiter.api.BeforeAll;

import static io.trino.SystemSessionProperties.MARK_DISTINCT_STRATEGY;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestDistinctAggregationsNoMarkDistinct
        extends TestDistinctAggregations
{
    @BeforeAll
    @Override
    public void init()
    {
        assertions = new QueryAssertions(testSessionBuilder()
                .setSystemProperty(MARK_DISTINCT_STRATEGY, "none")
                .build());
    }
}
