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

import io.airlift.bytecode.BytecodeNode;
import io.trino.sql.gen.LambdaBytecodeGenerator.CompiledLambda;
import io.trino.sql.relational.LambdaDefinitionExpression;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SpecialForm;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class BindCodeGenerator
        implements BytecodeGenerator
{
    private final Class<?> lambdaInterface;
    private final CompiledLambda compiledLambda;
    private final List<RowExpression> captureExpressions;

    public BindCodeGenerator(SpecialForm specialForm, Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap, Class<?> lambdaInterface)
    {
        requireNonNull(specialForm, "specialForm is null");
        requireNonNull(compiledLambdaMap, "compiledLambdaMap is null");

        this.lambdaInterface = requireNonNull(lambdaInterface, "lambdaInterface is null");

        // Bind expression is used to generate captured lambda.
        // It takes the captured values and the uncaptured lambda, and produces captured lambda as the output.
        // The uncaptured lambda is just a method, and does not have a stack representation during execution.
        // As a result, the bind expression generates the captured lambda in one step.
        List<RowExpression> arguments = specialForm.arguments();
        int numCaptures = arguments.size() - 1;
        LambdaDefinitionExpression lambda = (LambdaDefinitionExpression) arguments.get(numCaptures);
        checkArgument(compiledLambdaMap.containsKey(lambda), "lambda expressions map does not contain this lambda definition");
        compiledLambda = compiledLambdaMap.get(lambda);
        captureExpressions = arguments.subList(0, numCaptures);
    }

    @Override
    public BytecodeNode generateExpression(BytecodeGeneratorContext context)
    {
        return LambdaBytecodeGenerator.generateLambda(
                context,
                captureExpressions,
                compiledLambda,
                lambdaInterface);
    }
}
