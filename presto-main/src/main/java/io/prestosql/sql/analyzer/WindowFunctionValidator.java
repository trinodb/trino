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
package io.prestosql.sql.analyzer;

import io.prestosql.metadata.ResolvedFunction;
import io.prestosql.sql.tree.DefaultExpressionTraversalVisitor;
import io.prestosql.sql.tree.FunctionCall;

import static io.prestosql.metadata.FunctionKind.WINDOW;
import static io.prestosql.spi.StandardErrorCode.MISSING_OVER;
import static io.prestosql.sql.analyzer.SemanticExceptions.semanticException;
import static java.util.Objects.requireNonNull;

class WindowFunctionValidator
        extends DefaultExpressionTraversalVisitor<Void, Analysis>
{
    @Override
    protected Void visitFunctionCall(FunctionCall functionCall, Analysis analysis)
    {
        requireNonNull(analysis, "analysis is null");

        ResolvedFunction resolvedFunction = analysis.getResolvedFunction(functionCall);
        if (resolvedFunction != null && resolvedFunction.getSignature().getKind() == WINDOW && !functionCall.getWindow().isPresent()) {
            throw semanticException(MISSING_OVER, functionCall, "Window function %s requires an OVER clause", resolvedFunction.getSignature().getName());
        }
        return super.visitFunctionCall(functionCall, analysis);
    }
}
