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
package io.trino.faulttolerant;

import io.trino.Session;
import io.trino.execution.BaseTestJoinShortCircuit;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.FaultTolerantExecutionConnectorTestHelper;
import org.junit.jupiter.api.parallel.Execution;

import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
final class TestFaultTolerantJoinShortCircuit
        extends BaseTestJoinShortCircuit
{
    @Override
    protected DistributedQueryRunner.Builder<?> createQueryRunnerBuilder(Session defaultSession)
    {
        return DistributedQueryRunner.builder(defaultSession)
                .setExtraProperties(FaultTolerantExecutionConnectorTestHelper.getExtraProperties())
                .withExchange("filesystem");
    }

    @Override
    protected boolean isDynamicFilteringEnabled()
    {
        return false;
    }

    @Override
    protected boolean isScanStoppedWithJoin()
    {
        return false;
    }
}
