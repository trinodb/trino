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
package io.trino.operator;

import io.trino.Session;
import io.trino.execution.TaskId;

import static java.util.Objects.requireNonNull;

public class ProcessorContext
{
    private final Session session;
    private final OperatorContext operatorContext;
    private final DriverYieldSignal driverYieldSignal;
    private final SpillContext spillContext;
    private final TaskId taskId;

    public ProcessorContext(Session session, OperatorContext operatorContext)
    {
        this.session = requireNonNull(session, "session is null");
        requireNonNull(operatorContext, "operatorContext is null");
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.driverYieldSignal = operatorContext.getDriverContext().getYieldSignal();
        this.spillContext = operatorContext.getSpillContext();
        this.taskId = operatorContext.getDriverContext().getTaskId();
    }

    public Session getSession()
    {
        return session;
    }

    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    public DriverYieldSignal getDriverYieldSignal()
    {
        return driverYieldSignal;
    }

    public SpillContext getSpillContext()
    {
        return spillContext;
    }

    public TaskId getTaskId()
    {
        return taskId;
    }
}
