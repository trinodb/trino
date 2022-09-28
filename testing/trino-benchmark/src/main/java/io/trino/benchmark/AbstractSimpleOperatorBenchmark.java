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
package io.trino.benchmark;

import com.google.common.collect.ImmutableList;
import io.trino.operator.Driver;
import io.trino.operator.DriverContext;
import io.trino.operator.DriverFactory;
import io.trino.operator.OperatorFactory;
import io.trino.operator.TaskContext;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.NullOutputOperator.NullOutputOperatorFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;

public abstract class AbstractSimpleOperatorBenchmark
        extends AbstractOperatorBenchmark
{
    protected AbstractSimpleOperatorBenchmark(
            LocalQueryRunner localQueryRunner,
            String benchmarkName,
            int warmupIterations,
            int measuredIterations)
    {
        super(localQueryRunner, benchmarkName, warmupIterations, measuredIterations);
    }

    protected abstract List<? extends OperatorFactory> createOperatorFactories();

    protected DriverFactory createDriverFactory()
    {
        List<OperatorFactory> operatorFactories = new ArrayList<>(createOperatorFactories());

        operatorFactories.add(new NullOutputOperatorFactory(999, new PlanNodeId("test")));

        return new DriverFactory(0, true, true, operatorFactories, OptionalInt.empty());
    }

    @Override
    protected List<Driver> createDrivers(TaskContext taskContext)
    {
        DriverFactory driverFactory = createDriverFactory();
        DriverContext driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
        Driver driver = driverFactory.createDriver(driverContext);
        return ImmutableList.of(driver);
    }
}
