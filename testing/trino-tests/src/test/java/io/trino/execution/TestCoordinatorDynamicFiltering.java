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
package io.trino.execution;

import com.google.common.collect.ImmutableMap;
import io.trino.operator.RetryPolicy;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static io.trino.operator.RetryPolicy.NONE;

@Test(singleThreaded = true)
public class TestCoordinatorDynamicFiltering
        extends AbstractTestCoordinatorDynamicFiltering
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return DistributedQueryRunner.builder(getDefaultSession())
                .setExtraProperties(ImmutableMap.of(
                        "retry-policy", getRetryPolicy().name(),
                        // keep limits lower to test edge cases
                        "dynamic-filtering.small-partitioned.max-distinct-values-per-driver", "10",
                        "dynamic-filtering.small.max-distinct-values-per-driver", "10"))
                .build();
    }

    @Override
    protected RetryPolicy getRetryPolicy()
    {
        return NONE;
    }
}
