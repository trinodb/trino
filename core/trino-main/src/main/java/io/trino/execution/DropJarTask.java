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

import com.google.common.util.concurrent.ListenableFuture;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.FunctionJarDynamicManager;
import io.trino.sql.tree.DropJar;
import io.trino.sql.tree.Expression;

import javax.inject.Inject;

import java.util.List;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static java.util.Objects.requireNonNull;

public class DropJarTask
        implements DataDefinitionTask<DropJar>
{
    private final FunctionJarDynamicManager functionJarDynamicManager;

    @Inject
    public DropJarTask(FunctionJarDynamicManager functionJarDynamicManager)
    {
        this.functionJarDynamicManager = requireNonNull(functionJarDynamicManager, "functionManager is null");
    }

    @Override
    public String getName()
    {
        return "DROP JAR";
    }

    @Override
    public ListenableFuture<Void> execute(
            DropJar statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        functionJarDynamicManager.dropJar(statement.getJarName(), statement.isExists());
        return immediateVoidFuture();
    }
}
