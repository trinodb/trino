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
package io.trino.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.metadata.SqlScalarFunction;
import io.trino.operator.scalar.ChoicesSpecializedSqlScalarFunction.ScalarImplementationChoice;
import io.trino.operator.scalar.ConstantSpecializedSqlScalarFunction.Specialization;
import io.trino.operator.scalar.annotations.ParametricScalarImplementation.ConstantImplementationChoice;
import io.trino.spi.block.BufferedArrayValueBuilder;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.InvocationConvention.InvocationArgumentConvention;
import io.trino.spi.function.InvocationConvention.InvocationReturnConvention;
import io.trino.spi.function.Signature;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.TypeTemplate;
import io.trino.sql.gen.lambda.UnaryFunctionInterface;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.operator.scalar.ConstantPositions.remainingSignature;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FUNCTION;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.type.Chars.padSpaces;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeDescriptorTranslator.parseTypeTemplate;
import static io.trino.type.TypeCalculation.parseNumericExpression;
import static io.trino.util.Reflection.constructorMethodHandle;
import static io.trino.util.Reflection.methodHandle;
import static java.lang.invoke.MethodHandles.collectArguments;
import static java.lang.invoke.MethodHandles.filterReturnValue;
import static java.lang.invoke.MethodHandles.insertArguments;
import static java.lang.invoke.MethodHandles.permuteArguments;
import static java.util.Objects.requireNonNull;

/**
 * The regexp_* functions, shared by all regex libraries.
 * <p>
 * The pattern is argument 1. When it is a constant, the pattern is compiled once per function instance
 * and the remaining arguments are evaluated against it. Otherwise the pattern is compiled for every row.
 * Every function is registered for {@code varchar} patterns and for {@code char} patterns,
 * which keep their trailing spaces.
 */
public final class RegexpFunctions
{
    private static final int PATTERN_POSITION = 1;
    private static final Set<Integer> PATTERN_POSITIONS = Set.of(PATTERN_POSITION);
    private static final Set<String> NUMERIC_VARIABLES = Set.of("x", "y", "z", "p");
    private static final String REPLACE_LENGTH = "min(2147483647, x + max(x * y / 2, y) * (x + 1))";

    private static final MethodHandle COMPILE = methodHandle(RegexpFunctions.class, "compile", Function.class, int.class, Slice.class);
    private static final MethodHandle NEW_REPLACE_LAMBDA = constructorMethodHandle(ReplaceLambda.class, Regexp.class);

    private static final MethodHandle LIKE = methodHandle(Regexp.class, "like", Slice.class);
    private static final MethodHandle REPLACE = methodHandle(Regexp.class, "replace", Slice.class, Slice.class);
    private static final MethodHandle REPLACE_LAMBDA = methodHandle(ReplaceLambda.class, "replace", Slice.class, UnaryFunctionInterface.class);
    private static final MethodHandle EXTRACT_ALL = methodHandle(Regexp.class, "extractAll", Slice.class, long.class);
    private static final MethodHandle EXTRACT = methodHandle(Regexp.class, "extract", Slice.class, long.class);
    private static final MethodHandle SPLIT = methodHandle(Regexp.class, "split", Slice.class);
    private static final MethodHandle POSITION = methodHandle(Regexp.class, "position", Slice.class, long.class, long.class);
    private static final MethodHandle COUNT = methodHandle(Regexp.class, "count", Slice.class);

    private static final List<Definition> DEFINITIONS = ImmutableList.of(
            new Definition("regexp_like", "Returns whether the pattern is contained within the string", "boolean", "varchar(x)", List.of(), false, LIKE),
            new Definition(
                    "regexp_replace",
                    "Removes substrings matching a regular expression",
                    "varchar(x)",
                    "varchar(x)",
                    List.of(),
                    false,
                    insertArguments(REPLACE, 2, Slices.EMPTY_SLICE)),
            new Definition("regexp_replace", "Replaces substrings matching a regular expression by given string", "varchar(z)", "varchar(x)", List.of("varchar(y)"), false, REPLACE),
            new Definition("regexp_replace", "Replaces substrings matching a regular expression using a lambda function", "varchar", "varchar", List.of("function(array(varchar), varchar(x))"), true, REPLACE_LAMBDA),
            new Definition(
                    "regexp_extract_all",
                    "String(s) extracted using the given pattern",
                    "array(varchar(x))",
                    "varchar(x)",
                    List.of(),
                    false,
                    insertArguments(EXTRACT_ALL, 2, 0L)),
            new Definition("regexp_extract_all", "Group(s) extracted using the given pattern", "array(varchar(x))", "varchar(x)", List.of("bigint"), false, EXTRACT_ALL),
            new Definition(
                    "regexp_extract",
                    "String extracted using the given pattern",
                    "varchar(x)",
                    "varchar(x)",
                    List.of(),
                    true,
                    insertArguments(EXTRACT, 2, 0L)),
            new Definition("regexp_extract", "Returns regex group of extracted string with a pattern", "varchar(x)", "varchar(x)", List.of("bigint"), true, EXTRACT),
            new Definition("regexp_split", "Returns array of strings split by pattern", "array(varchar(x))", "varchar(x)", List.of(), false, SPLIT),
            new Definition(
                    "regexp_position",
                    "Returns the index of the matched substring",
                    "integer",
                    "varchar(x)",
                    List.of(),
                    false,
                    insertArguments(POSITION, 2, 1L, 1L)),
            new Definition(
                    "regexp_position",
                    "Returns the index of the matched substring starting from the specified position",
                    "integer",
                    "varchar(x)",
                    List.of("integer"),
                    false,
                    insertArguments(POSITION, 3, 1L)),
            new Definition("regexp_position", "Returns the index of the n-th matched substring starting from the specified position", "integer", "varchar(x)", List.of("integer", "integer"), false, POSITION),
            new Definition("regexp_count", "Returns the number of times that a pattern occurs in a string", "bigint", "varchar(x)", List.of(), false, COUNT));

    private RegexpFunctions() {}

    /**
     * @param compiler compiles a pattern, failing with a {@link io.trino.spi.TrinoException} when the pattern is invalid
     */
    public static List<SqlScalarFunction> regexpFunctions(Function<Slice, Regexp> compiler)
    {
        requireNonNull(compiler, "compiler is null");
        ImmutableList.Builder<SqlScalarFunction> functions = ImmutableList.builder();
        for (Definition definition : DEFINITIONS) {
            functions.add(new RegexpFunction(definition, "varchar(p)", compiler));
            functions.add(new RegexpFunction(definition, "char(p)", compiler));
        }
        return functions.build();
    }

    public static Regexp compile(Function<Slice, Regexp> compiler, int charLength, Slice pattern)
    {
        return compiler.apply(charLength < 0 ? pattern : padSpaces(pattern, charLength));
    }

    private record Definition(
            String name,
            String description,
            String returnType,
            String sourceType,
            List<String> trailingArgumentTypes,
            boolean nullable,
            // (receiver, source, trailing arguments...), where the receiver is either a Regexp or a ReplaceLambda
            MethodHandle operation)
    {
        boolean isLambda()
        {
            return operation.type().parameterType(0) == ReplaceLambda.class;
        }
    }

    private static final class RegexpFunction
            extends SqlScalarFunction
    {
        private final Definition definition;
        private final Function<Slice, Regexp> compiler;

        private RegexpFunction(Definition definition, String patternType, Function<Slice, Regexp> compiler)
        {
            super(metadata(definition, patternType));
            this.definition = definition;
            this.compiler = compiler;
        }

        private static FunctionMetadata metadata(Definition definition, String patternType)
        {
            Signature.Builder signature = Signature.builder()
                    .returnType(template(definition.returnType()))
                    .argumentType(template(definition.sourceType()))
                    .argumentType(template(patternType));
            definition.trailingArgumentTypes().forEach(type -> signature.argumentType(template(type)));

            String allTypes = definition.returnType() + definition.sourceType() + patternType + String.join("", definition.trailingArgumentTypes());
            for (String variable : NUMERIC_VARIABLES) {
                if (!allTypes.contains("(" + variable + ")")) {
                    continue;
                }
                if (variable.equals("z")) {
                    signature.numericVariable(variable, parseNumericExpression(REPLACE_LENGTH));
                }
                else {
                    signature.numericVariable(variable);
                }
            }

            FunctionMetadata.Builder metadata = FunctionMetadata.scalarBuilder(definition.name())
                    .signature(signature.build())
                    .description(definition.description());
            if (definition.nullable()) {
                metadata.nullable();
            }
            return metadata.build();
        }

        private static TypeTemplate template(String type)
        {
            return parseTypeTemplate(type, Set.of(), NUMERIC_VARIABLES);
        }

        @Override
        public SpecializedSqlScalarFunction specialize(BoundSignature boundSignature, FunctionDependencies functionDependencies)
        {
            int charLength = boundSignature.getArgumentType(PATTERN_POSITION) instanceof CharType charType ? charType.getLength() : -1;
            MethodHandle compile = insertArguments(COMPILE, 0, compiler, charLength);
            MethodHandle instanceFactory = definition.isLambda() ? filterReturnValue(compile, NEW_REPLACE_LAMBDA) : compile;

            InvocationReturnConvention returnConvention = definition.nullable() ? NULLABLE_RETURN : FAIL_ON_NULL;
            List<Class<?>> lambdaInterfaces = definition.isLambda() ? List.of(UnaryFunctionInterface.class) : List.of();
            List<InvocationArgumentConvention> trailingConventions = definition.trailingArgumentTypes().stream()
                    .map(type -> type.startsWith("function") ? FUNCTION : NEVER_NULL)
                    .collect(toImmutableList());
            List<InvocationArgumentConvention> constantConventions = ImmutableList.<InvocationArgumentConvention>builder()
                    .add(NEVER_NULL)
                    .addAll(trailingConventions)
                    .build();
            List<InvocationArgumentConvention> rowConventions = ImmutableList.<InvocationArgumentConvention>builder()
                    .add(NEVER_NULL, NEVER_NULL)
                    .addAll(trailingConventions)
                    .build();

            // (pattern, source, trailing...) -> (source, pattern, trailing...)
            MethodHandle patternFirst = collectArguments(definition.operation(), 0, instanceFactory);
            int[] reorder = new int[patternFirst.type().parameterCount()];
            for (int i = 0; i < reorder.length; i++) {
                reorder[i] = i;
            }
            reorder[0] = 1;
            reorder[1] = 0;
            MethodHandle row = permuteArguments(patternFirst, patternFirst.type().changeParameterType(0, patternFirst.type().parameterType(1)).changeParameterType(1, patternFirst.type().parameterType(0)), reorder);

            SpecializedSqlScalarFunction nonConstantImplementation = new ChoicesSpecializedSqlScalarFunction(
                    boundSignature,
                    ImmutableList.of(new ScalarImplementationChoice(returnConvention, rowConventions, lambdaInterfaces, row, Optional.empty())));
            Specialization specialization = new Specialization(
                    PATTERN_POSITIONS,
                    remainingSignature(boundSignature, PATTERN_POSITIONS),
                    List.of(new ConstantImplementationChoice(returnConvention, constantConventions, lambdaInterfaces, definition.operation(), instanceFactory)),
                    List.of(PATTERN_POSITION));
            return new ConstantSpecializedSqlScalarFunction(nonConstantImplementation, List.of(specialization));
        }
    }

    /**
     * Holds the per-instance state needed to call a lambda for each match.
     */
    public static final class ReplaceLambda
    {
        private final Regexp regexp;
        private final BufferedArrayValueBuilder arrayValueBuilder = BufferedArrayValueBuilder.createBuffered(new ArrayType(VARCHAR));

        public ReplaceLambda(Regexp regexp)
        {
            this.regexp = requireNonNull(regexp, "regexp is null");
        }

        public Slice replace(Slice source, UnaryFunctionInterface replacer)
        {
            return regexp.replace(source, replacer, arrayValueBuilder);
        }
    }
}
