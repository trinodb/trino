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
package io.trino.sql.planner;

import io.trino.operator.DriverContext;
import io.trino.operator.LocalPlannerAware;
import io.trino.operator.Operator;
import io.trino.operator.OperatorFactory;

import static java.util.Objects.requireNonNull;

/**
 * OperatorFactory that can be reused between DriverFactory instances that share the same lifecycle.
 * DriverFactory instances are reused by different sub-plan alternatives for the same pipeline.
 * This class makes sure noMoreOperators is called only once for the delegate
 * as some OperatorFactory implementations fail if noMoreOperators is called twice.
 */
public class SharedOperatorFactory
        implements OperatorFactory, LocalPlannerAware
{
    private final OperatorFactory delegate;
    private boolean noMoreOperators;

    public SharedOperatorFactory(OperatorFactory delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public Operator createOperator(DriverContext driverContext)
    {
        return delegate.createOperator(driverContext);
    }

    @Override
    public synchronized void noMoreOperators()
    {
        if (noMoreOperators) {
            return;
        }
        delegate.noMoreOperators();
        noMoreOperators = true;
    }

    @Override
    public OperatorFactory duplicate()
    {
        return new SharedOperatorFactory(delegate.duplicate());
    }

    @Override
    public void localPlannerComplete()
    {
        if (delegate instanceof LocalPlannerAware localPlannerAware) {
            localPlannerAware.localPlannerComplete();
        }
    }
}
