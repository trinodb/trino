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

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.trino.metadata.FunctionManager;
import io.trino.spi.type.Type;
import io.trino.sql.gen.LambdaBytecodeGenerator.CompiledLambda;
import io.trino.sql.relational.CallExpression;
import io.trino.sql.relational.ConstantExpression;
import io.trino.sql.relational.InputReferenceExpression;
import io.trino.sql.relational.LambdaDefinitionExpression;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.RowExpressionVisitor;
import io.trino.sql.relational.SpecialForm;
import io.trino.sql.relational.VariableReferenceExpression;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantTrue;
import static io.airlift.bytecode.instruction.Constant.loadBoolean;
import static io.airlift.bytecode.instruction.Constant.loadDouble;
import static io.airlift.bytecode.instruction.Constant.loadLong;
import static io.airlift.bytecode.instruction.Constant.loadString;
import static io.trino.sql.gen.BytecodeUtils.loadConstant;
import static io.trino.sql.gen.LambdaBytecodeGenerator.generateLambda;

public class RowExpressionCompiler
{
    private final CallSiteBinder callSiteBinder;
    private final CachedInstanceBinder cachedInstanceBinder;
    private final RowExpressionVisitor<BytecodeNode, Scope> fieldReferenceCompiler;
    private final FunctionManager functionManager;
    private final Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap;

    RowExpressionCompiler(
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            RowExpressionVisitor<BytecodeNode, Scope> fieldReferenceCompiler,
            FunctionManager functionManager,
            Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap)
    {
        this.callSiteBinder = callSiteBinder;
        this.cachedInstanceBinder = cachedInstanceBinder;
        this.fieldReferenceCompiler = fieldReferenceCompiler;
        this.functionManager = functionManager;
        this.compiledLambdaMap = compiledLambdaMap;
    }

    public BytecodeNode compile(RowExpression rowExpression, Scope scope)
    {
        return compile(rowExpression, scope, Optional.empty());
    }

    public BytecodeNode compile(RowExpression rowExpression, Scope scope, Optional<Class<?>> lambdaInterface)
    {
        return rowExpression.accept(new Visitor(), new Context(scope, lambdaInterface));
    }

    private class Visitor
            implements RowExpressionVisitor<BytecodeNode, Context>
    {
        @Override
        public BytecodeNode visitCall(CallExpression call, Context context)
        {
            BytecodeGeneratorContext generatorContext = new BytecodeGeneratorContext(
                    RowExpressionCompiler.this,
                    context.getScope(),
                    callSiteBinder,
                    cachedInstanceBinder,
                    functionManager);

            return generatorContext.generateFullCall(call.getResolvedFunction(), call.getArguments());
        }

        @Override
        public BytecodeNode visitSpecialForm(SpecialForm specialForm, Context context)
        {
            BytecodeGenerator generator;
            // special-cased in function registry
            switch (specialForm.getForm()) {
                // lazy evaluation
                case IF:
                    generator = new IfCodeGenerator(specialForm);
                    break;
                case NULL_IF:
                    generator = new NullIfCodeGenerator(specialForm);
                    break;
                case SWITCH:
                    // (SWITCH <expr> (WHEN <expr> <expr>) (WHEN <expr> <expr>) <expr>)
                    generator = new SwitchCodeGenerator(specialForm);
                    break;
                case BETWEEN:
                    generator = new BetweenCodeGenerator(specialForm);
                    break;
                // functions that take null as input
                case IS_NULL:
                    generator = new IsNullCodeGenerator(specialForm);
                    break;
                case COALESCE:
                    generator = new CoalesceCodeGenerator(specialForm);
                    break;
                // functions that require varargs and/or complex types (e.g., lists)
                case IN:
                    generator = new InCodeGenerator(specialForm);
                    break;
                // optimized implementations (shortcircuiting behavior)
                case AND:
                    generator = new AndCodeGenerator(specialForm);
                    break;
                case OR:
                    generator = new OrCodeGenerator(specialForm);
                    break;
                case DEREFERENCE:
                    generator = new DereferenceCodeGenerator(specialForm);
                    break;
                case ROW_CONSTRUCTOR:
                    generator = new RowConstructorCodeGenerator(specialForm);
                    break;
                case BIND:
                    generator = new BindCodeGenerator(specialForm, compiledLambdaMap, context.getLambdaInterface().get());
                    break;
                default:
                    throw new IllegalStateException("Cannot compile special form: " + specialForm.getForm());
            }

            BytecodeGeneratorContext generatorContext = new BytecodeGeneratorContext(
                    RowExpressionCompiler.this,
                    context.getScope(),
                    callSiteBinder,
                    cachedInstanceBinder,
                    functionManager);

            return generator.generateExpression(generatorContext);
        }

        @Override
        public BytecodeNode visitConstant(ConstantExpression constant, Context context)
        {
            Object value = constant.getValue();
            Class<?> javaType = constant.getType().getJavaType();

            BytecodeBlock block = new BytecodeBlock();
            if (value == null) {
                return block.comment("constant null")
                        .append(context.getScope().getVariable("wasNull").set(constantTrue()))
                        .pushJavaDefault(javaType);
            }

            // use LDC for primitives (boolean, short, int, long, float, double)
            block.comment("constant " + constant.getType().getTypeSignature());
            if (javaType == boolean.class) {
                return block.append(loadBoolean((Boolean) value));
            }
            if (javaType == long.class) {
                return block.append(loadLong((Long) value));
            }
            if (javaType == double.class) {
                return block.append(loadDouble((Double) value));
            }
            if (javaType == String.class) {
                return block.append(loadString((String) value));
            }

            // bind constant object directly into the call-site using invoke dynamic
            Binding binding = callSiteBinder.bind(value, constant.getType().getJavaType());

            return new BytecodeBlock()
                    .setDescription("constant " + constant.getType())
                    .comment(constant.toString())
                    .append(loadConstant(binding));
        }

        @Override
        public BytecodeNode visitInputReference(InputReferenceExpression node, Context context)
        {
            return fieldReferenceCompiler.visitInputReference(node, context.getScope());
        }

        @Override
        public BytecodeNode visitLambda(LambdaDefinitionExpression lambda, Context context)
        {
            checkState(compiledLambdaMap.containsKey(lambda), "lambda expressions map does not contain this lambda definition");
            if (!context.lambdaInterface.get().isAnnotationPresent(FunctionalInterface.class)) {
                // lambdaInterface is checked to be annotated with FunctionalInterface when generating ScalarFunctionImplementation
                throw new VerifyException("lambda should be generated as class annotated with FunctionalInterface");
            }

            BytecodeGeneratorContext generatorContext = new BytecodeGeneratorContext(
                    RowExpressionCompiler.this,
                    context.getScope(),
                    callSiteBinder,
                    cachedInstanceBinder,
                    functionManager);

            return generateLambda(
                    generatorContext,
                    ImmutableList.of(),
                    compiledLambdaMap.get(lambda),
                    context.getLambdaInterface().get());
        }

        @Override
        public BytecodeNode visitVariableReference(VariableReferenceExpression reference, Context context)
        {
            if (reference.getName().startsWith(TEMP_PREFIX)) {
                return context.getScope().getTempVariable(reference.getName().substring(TEMP_PREFIX.length()));
            }
            return fieldReferenceCompiler.visitVariableReference(reference, context.getScope());
        }
    }

    private static final String TEMP_PREFIX = "$$TEMP$$";

    public static VariableReferenceExpression createTempVariableReferenceExpression(Variable variable, Type type)
    {
        return new VariableReferenceExpression(TEMP_PREFIX + variable.getName(), type);
    }

    private static class Context
    {
        private final Scope scope;
        private final Optional<Class<?>> lambdaInterface;

        public Context(Scope scope, Optional<Class<?>> lambdaInterface)
        {
            this.scope = scope;
            this.lambdaInterface = lambdaInterface;
        }

        public Scope getScope()
        {
            return scope;
        }

        public Optional<Class<?>> getLambdaInterface()
        {
            return lambdaInterface;
        }
    }
}
