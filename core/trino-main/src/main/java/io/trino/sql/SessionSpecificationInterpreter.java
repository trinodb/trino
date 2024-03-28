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
package io.trino.sql;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Table;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Literal;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.SessionSpecification;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.StringLiteral;

import java.util.List;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.util.Objects.requireNonNull;

public class SessionSpecificationInterpreter
{
    private SessionSpecificationInterpreter() {}

    public static ResolvedSessionSpecifications evaluate(Statement statement)
    {
        if (statement instanceof Query queryStatement) {
            return evaluate(queryStatement.getSessionProperties());
        }

        return new ResolvedSessionSpecifications(ImmutableMap.of(), ImmutableMap.of());
    }

    private static ResolvedSessionSpecifications evaluate(List<SessionSpecification> specifications)
    {
        ImmutableMap.Builder<String, String> sessionProperties = ImmutableMap.builder();
        Table<String, String, String> catalogProperties = HashBasedTable.create();

        for (SessionSpecification specification : specifications) {
            List<String> nameParts = specification.getName().getParts();
            String propertyValue = decodeLiteral(specification.getExpression());

            if (propertyValue == null) {
                throw semanticException(INVALID_SESSION_PROPERTY, specification.getExpression(), "Session property %s is null", specification.getName());
            }

            if (nameParts.size() == 1) {
                sessionProperties.put(nameParts.getFirst(), propertyValue);
            }

            if (nameParts.size() == 2) {
                catalogProperties.put(nameParts.get(0), nameParts.get(1), propertyValue);
            }
        }

        return new ResolvedSessionSpecifications(sessionProperties.buildOrThrow(), catalogProperties.rowMap());
    }

    private static String decodeLiteral(Expression expression)
    {
        if (!(expression instanceof Literal)) {
            throw semanticException(INVALID_SESSION_PROPERTY, expression, "Session property %s is not a literal", expression);
        }

        switch (expression) {
            case BooleanLiteral booleanLiteral -> {
                return Boolean.toString(booleanLiteral.getValue());
            }
            case StringLiteral stringLiteral -> {
                return stringLiteral.getValue();
            }
            case DecimalLiteral decimalLiteral -> {
                return decimalLiteral.getValue();
            }
            case NullLiteral ignored -> {
                return null;
            }
            case LongLiteral longLiteral -> {
                return Long.toString(longLiteral.getParsedValue());
            }
            case DoubleLiteral doubleLiteral -> {
                return Double.toString(doubleLiteral.getValue());
            }
            default -> throw semanticException(INVALID_SESSION_PROPERTY, expression, "Session property %s is not supported", expression);
        }
    }

    public record ResolvedSessionSpecifications(Map<String, String> systemProperties, Map<String, Map<String, String>> catalogProperties)
    {
        public ResolvedSessionSpecifications
        {
            systemProperties = ImmutableMap.copyOf(requireNonNull(systemProperties, "systemProperties is null"));
            catalogProperties = ImmutableMap.copyOf(requireNonNull(catalogProperties, "catalogProperties is null"));
        }
    }
}
