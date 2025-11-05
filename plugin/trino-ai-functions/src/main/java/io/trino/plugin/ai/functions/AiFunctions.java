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
package io.trino.plugin.ai.functions;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlMap;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.ScalarFunctionAdapter;
import io.trino.spi.function.ScalarFunctionImplementation;
import io.trino.spi.function.Signature;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeSignature;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.block.MapValueBuilder.buildMapValue;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.TypeSignature.arrayType;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

public class AiFunctions
        implements FunctionProvider
{
    private static final TypeSignature TEXT = VARCHAR.getTypeSignature();
    private static final List<FunctionMetadata> FUNCTIONS = ImmutableList.<FunctionMetadata>builder()
            .add(function("ai_analyze_sentiment")
                    .description("Perform sentiment analysis on text")
                    .signature(signature(TEXT, TEXT))
                    .build())
            .add(function("ai_classify")
                    .description("Classify text with the provided labels")
                    .signature(signature(TEXT, TEXT, arrayType(TEXT)))
                    .build())
            .add(function("ai_extract")
                    .description("Extract values for the provided labels from text")
                    .signature(signature(mapType(TEXT, TEXT), TEXT, arrayType(TEXT)))
                    .build())
            .add(function("ai_fix_grammar")
                    .description("Correct grammatical errors in text")
                    .signature(signature(TEXT, TEXT))
                    .build())
            .add(function("ai_gen")
                    .description("Generate text based on a prompt")
                    .signature(signature(TEXT, TEXT))
                    .build())
            .add(function("ai_mask")
                    .description("Mask values for the provided labels in text")
                    .signature(signature(TEXT, TEXT, arrayType(TEXT)))
                    .build())
            .add(function("ai_translate")
                    .description("Translate text to the specified language")
                    .signature(signature(TEXT, TEXT, TEXT))
                    .build())
            .build();

    private static final MethodHandle AI_ANALYZE_SENTIMENT;
    private static final MethodHandle AI_CLASSIFY;
    private static final MethodHandle AI_EXTRACT;
    private static final MethodHandle AI_FIX_GRAMMAR;
    private static final MethodHandle AI_GEN;
    private static final MethodHandle AI_MASK;
    private static final MethodHandle AI_TRANSLATE;

    static {
        try {
            AI_ANALYZE_SENTIMENT = lookup().findVirtual(AiFunctions.class, "aiAnalyzeSentiment", methodType(Slice.class, Slice.class));
            AI_CLASSIFY = lookup().findVirtual(AiFunctions.class, "aiClassify", methodType(Slice.class, Slice.class, Block.class));
            AI_EXTRACT = lookup().findVirtual(AiFunctions.class, "aiExtract", methodType(SqlMap.class, MapType.class, Slice.class, Block.class));
            AI_FIX_GRAMMAR = lookup().findVirtual(AiFunctions.class, "aiFixGrammar", methodType(Slice.class, Slice.class));
            AI_GEN = lookup().findVirtual(AiFunctions.class, "aiGen", methodType(Slice.class, Slice.class));
            AI_MASK = lookup().findVirtual(AiFunctions.class, "aiMask", methodType(Slice.class, Slice.class, Block.class));
            AI_TRANSLATE = lookup().findVirtual(AiFunctions.class, "aiTranslate", methodType(Slice.class, Slice.class, Slice.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final AiClient client;

    @Inject
    public AiFunctions(AiClient client)
    {
        this.client = requireNonNull(client, "client is null");
    }

    public List<FunctionMetadata> getFunctions()
    {
        return FUNCTIONS;
    }

    @Override
    public ScalarFunctionImplementation getScalarFunctionImplementation(
            FunctionId functionId,
            BoundSignature boundSignature,
            FunctionDependencies functionDependencies,
            InvocationConvention invocationConvention)
    {
        String name = functionId.toString();
        MethodHandle handle = switch (name) {
            case "ai_analyze_sentiment" -> AI_ANALYZE_SENTIMENT;
            case "ai_classify" -> AI_CLASSIFY;
            case "ai_extract" -> AI_EXTRACT;
            case "ai_fix_grammar" -> AI_FIX_GRAMMAR;
            case "ai_gen" -> AI_GEN;
            case "ai_mask" -> AI_MASK;
            case "ai_translate" -> AI_TRANSLATE;
            default -> throw new IllegalArgumentException("Invalid function ID: " + functionId);
        };

        handle = handle.bindTo(this);

        if (name.equals("ai_extract")) {
            handle = handle.bindTo(functionDependencies.getType(mapType(TEXT, TEXT)));
        }

        InvocationConvention actualConvention = new InvocationConvention(
                nCopies(boundSignature.getArity(), NEVER_NULL),
                FAIL_ON_NULL,
                false,
                false);

        handle = ScalarFunctionAdapter.adapt(
                handle,
                boundSignature.getReturnType(),
                boundSignature.getArgumentTypes(),
                actualConvention,
                invocationConvention);

        return ScalarFunctionImplementation.builder()
                .methodHandle(handle)
                .build();
    }

    public Slice aiAnalyzeSentiment(Slice text)
    {
        return utf8Slice(client.analyzeSentiment(text.toStringUtf8()));
    }

    public Slice aiClassify(Slice text, Block labels)
    {
        return utf8Slice(client.classify(text.toStringUtf8(), fromSqlArray(labels)));
    }

    public SqlMap aiExtract(MapType mapType, Slice text, Block labels)
    {
        return toSqlMap(mapType, client.extract(text.toStringUtf8(), fromSqlArray(labels)));
    }

    public Slice aiFixGrammar(Slice text)
    {
        return utf8Slice(client.fixGrammar(text.toStringUtf8()));
    }

    public Slice aiGen(Slice prompt)
    {
        return utf8Slice(client.generate(prompt.toStringUtf8()));
    }

    public Slice aiMask(Slice text, Block labels)
    {
        return utf8Slice(client.mask(text.toStringUtf8(), fromSqlArray(labels)));
    }

    public Slice aiTranslate(Slice text, Slice language)
    {
        return utf8Slice(client.translate(text.toStringUtf8(), language.toStringUtf8()));
    }

    private static List<String> fromSqlArray(Block block)
    {
        List<String> list = new ArrayList<>();
        for (int i = 0; i < block.getPositionCount(); i++) {
            list.add(VARCHAR.getSlice(block, i).toStringUtf8());
        }
        return list;
    }

    private static SqlMap toSqlMap(MapType type, Map<String, String> map)
    {
        return buildMapValue(type, map.size(), (keyBuilder, valueBuilder) ->
                map.forEach((key, value) -> {
                    VARCHAR.writeSlice(keyBuilder, utf8Slice(key));
                    if (value == null) {
                        valueBuilder.appendNull();
                    }
                    else {
                        VARCHAR.writeSlice(valueBuilder, utf8Slice(value));
                    }
                }));
    }

    private static FunctionMetadata.Builder function(String name)
    {
        return FunctionMetadata.scalarBuilder(name)
                .functionId(new FunctionId(name))
                .nondeterministic();
    }

    private static Signature signature(TypeSignature returnType, TypeSignature... argumentTypes)
    {
        return Signature.builder()
                .returnType(returnType)
                .argumentTypes(List.of(argumentTypes))
                .build();
    }
}
