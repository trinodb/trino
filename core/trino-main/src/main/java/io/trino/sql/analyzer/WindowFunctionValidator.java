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
package io.trino.sql.analyzer;

import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.sql.tree.DefaultExpressionTraversalVisitor;
import io.trino.sql.tree.FunctionCall;

import static io.trino.spi.StandardErrorCode.MISSING_OVER;
import static io.trino.spi.function.FunctionKind.WINDOW;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.util.Objects.requireNonNull;

class WindowFunctionValidator
        extends DefaultExpressionTraversalVisitor<Analysis>
{
    private final Session session;
    private final Metadata metadata;

    public WindowFunctionValidator(Session session, Metadata metadata)
    {
        this.session = session;
        this.metadata = metadata;
    }

    @Override
    protected Void visitFunctionCall(FunctionCall functionCall, Analysis analysis)
    {
        requireNonNull(analysis, "analysis is null");

        // pattern recognition functions are not resolved
        if (!analysis.isPatternRecognitionFunction(functionCall)) {
            ResolvedFunction resolvedFunction = analysis.getResolvedFunction(functionCall);
            if (resolvedFunction != null && functionCall.getWindow().isEmpty() && metadata.getFunctionMetadata(session, resolvedFunction).getKind() == WINDOW) {
                throw semanticException(MISSING_OVER, functionCall, "Window function %s requires an OVER clause", resolvedFunction.getSignature().getName());
            }
        }
        return super.visitFunctionCall(functionCall, analysis);
    }
}
