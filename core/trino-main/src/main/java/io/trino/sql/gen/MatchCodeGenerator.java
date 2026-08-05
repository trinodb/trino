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
package io.trino.sql.gen;

import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.IfStatement;
import io.trino.sql.ir.Bind;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionRewriter;
import io.trino.sql.ir.ExpressionTreeRewriter;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Match;
import io.trino.sql.ir.MatchClause;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.airlift.bytecode.expression.BytecodeExpressions.constantFalse;
import static io.trino.sql.gen.ExpressionBytecodeCompiler.createTempReference;
import static java.util.Objects.requireNonNull;

/// Generates bytecode for [Match]: evaluate the operand once into a temp variable, then test
/// each clause's lambda body with the parameter resolved to that temp. Single-evaluation of the
/// operand is the whole reason this node exists separately from a searched [io.trino.sql.ir.Case];
/// naively substituting the operand into each clause's predicate would re-evaluate it N times and
/// change semantics for non-deterministic operands such as `random()`.
///
/// For SQL three-valued logic, the operand's null state is captured into a separate flag at
/// evaluation time and restored to `wasNull` before each clause's predicate body runs, so
/// `WHEN IS NULL` and other null-aware predicates see the operand as null. The temp variable
/// holds the Java default when the operand was null; predicates that read it should already gate on
/// `wasNull` (the IR codegen convention) and will produce the right result.
public class MatchCodeGenerator
        implements BytecodeGenerator
{
    private final Expression value;
    private final List<MatchClause> clauses;
    private final Expression defaultValue;

    public MatchCodeGenerator(Match matchExpression)
    {
        requireNonNull(matchExpression, "matchExpression is null");
        value = matchExpression.operand();
        clauses = matchExpression.clauses();
        defaultValue = matchExpression.defaultValue();
    }

    @Override
    public BytecodeNode generateExpression(BytecodeGeneratorContext generatorContext)
    {
        Scope scope = generatorContext.getScope();
        CallSiteBinder callSiteBinder = generatorContext.getCallSiteBinder();

        BytecodeNode valueBytecode = generatorContext.generate(value);
        BytecodeNode elseValue = generatorContext.generate(defaultValue);

        Class<?> valueType = callSiteBinder.getAccessibleType(value.type().getJavaType());

        Variable tempVariable = scope.getOrCreateTempVariable(valueType);
        Variable operandWasNull = scope.getOrCreateTempVariable(boolean.class);
        Variable wasNull = generatorContext.wasNull();

        BytecodeBlock block = new BytecodeBlock()
                .append(valueBytecode)
                // After valueBytecode the operand value (or its java-default placeholder) sits on
                // the stack and wasNull reflects its nullness. Capture wasNull so each clause can
                // restore it; storing the value to the temp variable would otherwise leave us with
                // no record of whether it was null.
                .append(operandWasNull.set(wasNull))
                .putVariable(tempVariable)
                .append(wasNull.set(constantFalse()));

        Reference tempReference = createTempReference(tempVariable, value.type());

        // Build the if/else chain bottom-up because the if-statement builder doesn't support
        // if/else chains directly; each clause wraps the previously-built bytecode.
        BytecodeNode chain = elseValue;
        for (int i = clauses.size() - 1; i >= 0; i--) {
            MatchClause clause = clauses.get(i);
            Expression body = inlineBody(clause, tempReference);

            BytecodeBlock condition = new BytecodeBlock()
                    // Restore the operand's null-ness so the clause's predicate body sees it.
                    .append(wasNull.set(operandWasNull))
                    .append(generatorContext.generate(body))
                    // A null predicate result (SQL three-valued logic) is treated as not-matched.
                    .append(wasNull.set(constantFalse()));

            chain = new IfStatement("when")
                    .condition(condition)
                    .ifTrue(generatorContext.generate(clause.result()))
                    .ifFalse(chain);
        }

        block.append(chain);
        scope.releaseTempVariableForReuse(tempVariable);
        scope.releaseTempVariableForReuse(operandWasNull);
        return block;
    }

    /// Return the clause's lambda body with the operand parameter substituted by `tempReference`
    /// (so the body reads the once-evaluated operand) and any capture-desugared captured-parameters
    /// substituted back to their original outer references (so the body reads the outer scope
    /// directly). The result is suitable for inline bytecode generation in the surrounding method
    /// — the lambda interface call is dropped entirely.
    private static Expression inlineBody(MatchClause clause, Reference tempReference)
    {
        Lambda lambda = clause.lambda();
        Bind bind = clause.bind();
        List<Symbol> arguments = lambda.arguments();
        int captureCount = bind == null ? 0 : bind.values().size();
        // After LambdaCaptureDesugaringRewriter, the first captureCount args are captured-symbol
        // placeholders bound to the corresponding values in Bind.values; the remaining (always one
        // here, since Match's lambdas take a single operand parameter) is the operand parameter.
        Symbol operandParameter = arguments.getLast();

        Map<String, Expression> substitutions = new HashMap<>();
        substitutions.put(operandParameter.name(), tempReference);
        for (int i = 0; i < captureCount; i++) {
            substitutions.put(arguments.get(i).name(), bind.values().get(i));
        }

        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<>()
        {
            @Override
            public Expression rewriteReference(Reference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Expression replacement = substitutions.get(node.name());
                return replacement != null ? replacement : null;
            }

            @Override
            public Expression rewriteLambda(Lambda node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                // Don't recurse into nested lambdas that shadow any substituted name; if none of
                // the nested lambda's arguments shadows ours, fall through to keep recursing.
                if (node.arguments().stream().anyMatch(arg -> substitutions.containsKey(arg.name()))) {
                    return node;
                }
                return null;
            }
        }, lambda.body());
    }
}
