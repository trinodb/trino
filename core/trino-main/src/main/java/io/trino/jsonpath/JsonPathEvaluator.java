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
package io.trino.jsonpath;

import io.trino.json.Json;
import io.trino.jsonpath.ir.IrJsonPath;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.TypeManager;
import io.trino.sql.InterpretedFunctionInvoker;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Evaluates the JSON path expression using given JSON input and parameters,
 * respecting the path mode `strict` or `lax`.
 * Successful evaluation results in a sequence of [Json] values. A value is a JSON array,
 * object, scalar or null; a scalar the path engine produces itself (say, the result of
 * `$.foo + 1`) is a [TypedValue], which is a [Json] carrying an SQL type.
 * Certain error conditions might be suppressed in `lax` mode.
 * Any unsuppressed error condition causes evaluation failure.
 * In such case, `PathEvaluationError` is thrown.
 */
public class JsonPathEvaluator
{
    private final IrJsonPath path;
    private final Invoker invoker;
    private final CachingResolver resolver;

    public JsonPathEvaluator(IrJsonPath path, ConnectorSession session, Metadata metadata, TypeManager typeManager, FunctionManager functionManager)
    {
        requireNonNull(path, "path is null");
        requireNonNull(session, "session is null");
        requireNonNull(metadata, "metadata is null");
        requireNonNull(typeManager, "typeManager is null");
        requireNonNull(functionManager, "functionManager is null");

        this.path = path;
        this.invoker = new Invoker(session, functionManager);
        this.resolver = new CachingResolver(metadata);
    }

    public List<Json> evaluate(Json input, Object[] parameters)
    {
        return new PathEvaluationVisitor(path.isLax(), input, parameters, invoker, resolver).process(path.getRoot(), new PathEvaluationContext());
    }

    public static class Invoker
    {
        private final ConnectorSession connectorSession;
        private final InterpretedFunctionInvoker functionInvoker;

        public Invoker(ConnectorSession connectorSession, FunctionManager functionManager)
        {
            this.connectorSession = connectorSession;
            this.functionInvoker = new InterpretedFunctionInvoker(functionManager);
        }

        public Object invoke(ResolvedFunction function, List<Object> arguments)
        {
            return functionInvoker.invoke(function, connectorSession, arguments);
        }
    }
}
