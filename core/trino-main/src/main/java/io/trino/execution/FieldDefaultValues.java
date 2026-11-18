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
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.security.AccessControl;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;

import java.util.List;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.sql.analyzer.ExpressionAnalyzer.analyzeDefaultColumnValue;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;

final class FieldDefaultValues
{
    private FieldDefaultValues() {}

    /// Validates the defaults against the types of the fields they belong to and returns them as SQL literals.
    static Map<List<String>, String> analyzeFieldDefaults(
            Session session,
            PlannerContext plannerContext,
            AccessControl accessControl,
            Map<NodeRef<Parameter>, Expression> parameterLookup,
            WarningCollector warningCollector,
            Type columnType,
            Map<List<String>, Expression> defaults)
    {
        ImmutableMap.Builder<List<String>, String> result = ImmutableMap.builder();
        defaults.forEach((path, value) -> {
            Type fieldType = fieldType(columnType, path, value);
            analyzeDefaultColumnValue(session, plannerContext, accessControl, parameterLookup, warningCollector, fieldType, value);
            result.put(path, value.toString());
        });
        return result.buildOrThrow();
    }

    private static Type fieldType(Type columnType, List<String> path, Expression node)
    {
        Type current = columnType;
        for (String name : path) {
            if (!(current instanceof RowType rowType)) {
                throw semanticException(COLUMN_NOT_FOUND, node, "Field path %s does not exist within %s", path, columnType);
            }
            current = rowType.getFields().stream()
                    .filter(field -> field.getName().isPresent() && field.getName().get().equalsIgnoreCase(name))
                    .findFirst()
                    .orElseThrow(() -> semanticException(COLUMN_NOT_FOUND, node, "Field path %s does not exist within %s", path, columnType))
                    .getType();
        }
        return current;
    }
}
