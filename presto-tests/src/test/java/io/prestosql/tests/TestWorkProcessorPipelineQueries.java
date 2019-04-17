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
package io.prestosql.tests;

import io.prestosql.tests.tpch.TpchQueryRunnerBuilder;
import org.testng.annotations.Test;

import static io.prestosql.SystemSessionProperties.WORK_PROCESSOR_PIPELINES;

public class TestWorkProcessorPipelineQueries
        extends AbstractTestQueryFramework
{
    protected TestWorkProcessorPipelineQueries()
    {
        super(() -> TpchQueryRunnerBuilder
                .builder()
                .amendSession(builder -> builder.setSystemProperty(WORK_PROCESSOR_PIPELINES, "true"))
                .build());
    }

    @Test
    public void testTopN()
    {
        assertQuery("select * from orders order by totalprice limit 10");
    }
}
