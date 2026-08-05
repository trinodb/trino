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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.json.Json;
import io.trino.jsonpath.ir.IrContextVariable;
import io.trino.jsonpath.ir.IrJsonPath;
import io.trino.jsonpath.ir.IrMemberAccessor;
import io.trino.jsonpath.ir.IrPathNode;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.TypeManager;
import io.trino.sql.InterpretedFunctionInvoker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static io.trino.jsonpath.PathEvaluationException.itemTypeError;
import static io.trino.jsonpath.PathEvaluationException.structuralError;
import static io.trino.jsonpath.PathEvaluationUtil.unwrapArrays;
import static io.trino.jsonpath.PathEvaluationVisitor.itemTypeName;
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
    // Keys for the `$.a.b.c` member-accessor fast-path, pre-encoded once at
    // construction so the per-row hot loop compares UTF-8 bytes directly without
    // re-allocating either the key String or its UTF-8 encoding.
    private final Optional<Slice[]> memberAccessorChainKeys;

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
        this.memberAccessorChainKeys = getMemberAccessorChain(path.getRoot())
                .map(keys -> keys.stream().map(Slices::utf8Slice).toArray(Slice[]::new));
    }

    public List<Json> evaluate(Json input, Object[] parameters)
    {
        if (parameters.length == 0 && memberAccessorChainKeys.isPresent()) {
            return evaluateMemberAccessorChain(input, memberAccessorChainKeys.get(), path.isLax());
        }

        return new PathEvaluationVisitor(path.isLax(), input, parameters, invoker, resolver).process(path.getRoot(), new PathEvaluationContext());
    }

    /// Detects a path of the form `$.a.b.c` (a chain of by-key member accessors over the
    /// context variable). Returns the keys in path order. Wildcard accessors or any
    /// non-member node aborts detection and the general visitor handles the path.
    private static Optional<List<String>> getMemberAccessorChain(IrPathNode root)
    {
        List<String> keys = new ArrayList<>();
        IrPathNode current = root;
        while (current instanceof IrMemberAccessor accessor) {
            if (accessor.key().isEmpty()) {
                return Optional.empty();
            }
            keys.add(accessor.key().orElseThrow());
            current = accessor.base();
        }
        if (!(current instanceof IrContextVariable)) {
            return Optional.empty();
        }
        Collections.reverse(keys);
        return Optional.of(keys);
    }

    private static List<Json> evaluateMemberAccessorChain(Json input, Slice[] keys, boolean lax)
    {
        // Mirror IrMemberAccessor in PathEvaluationVisitor exactly, so this stays a pure
        // optimization rather than a second, divergent implementation: in lax mode each
        // accessor unwraps one array layer and skips non-objects; in strict mode every item
        // must be an object with the key present; and either way every matching member is
        // yielded, so duplicate keys (WITHOUT UNIQUE KEYS) are not dropped.
        List<Json> current = List.of(input);
        for (Slice key : keys) {
            List<Json> level = lax ? unwrapArrays(current) : current;
            List<Json> next = new ArrayList<>();
            for (Json item : level) {
                if (!item.isObject()) {
                    if (!lax) {
                        throw itemTypeError("OBJECT", itemTypeName(item));
                    }
                    continue;
                }
                int matchesBefore = next.size();
                item.objectMembers(key, next::add);
                if (next.size() == matchesBefore && !lax) {
                    throw structuralError("missing member '%s' in JSON object", key.toStringUtf8());
                }
            }
            current = next;
        }
        return current;
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
