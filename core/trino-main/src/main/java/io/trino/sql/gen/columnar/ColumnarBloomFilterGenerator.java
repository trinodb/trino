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
package io.trino.sql.gen.columnar;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.FieldDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.ForLoop;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.airlift.slice.Slice;
import io.trino.cache.NonEvictableCache;
import io.trino.operator.project.InputChannels;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.CharType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.objectweb.asm.MethodTooLargeException;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.google.common.base.Verify.verify;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.add;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.airlift.bytecode.expression.BytecodeExpressions.inlineIf;
import static io.airlift.bytecode.expression.BytecodeExpressions.lessThan;
import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.spi.StandardErrorCode.COMPILER_ERROR;
import static io.trino.spi.StandardErrorCode.QUERY_EXCEEDED_COMPILER_LIMIT;
import static io.trino.sql.gen.columnar.ColumnarFilterCompiler.updateOutputPositions;
import static io.trino.util.CompilerUtils.defineClass;
import static io.trino.util.CompilerUtils.makeClassName;
import static io.trino.util.Reflection.constructorMethodHandle;

public class ColumnarBloomFilterGenerator
{
    // Generate a ColumnarBloomFilter class per Type to avoid mega-morphic call site when reading a position from input block
    private static final NonEvictableCache<Type, MethodHandle> COLUMNAR_BLOOM_FILTER_CACHE = buildNonEvictableCache(
            CacheBuilder.newBuilder()
                    .maximumSize(100)
                    .expireAfterWrite(2, TimeUnit.HOURS));

    private ColumnarBloomFilterGenerator() {}

    public static boolean canUseBloomFilter(Domain domain)
    {
        Type type = domain.getType();
        if (type instanceof VarcharType || type instanceof CharType || type instanceof VarbinaryType) {
            verify(type.getJavaType() == Slice.class, "Type is not backed by Slice");
            return !domain.isNone()
                    && !domain.isAll()
                    && domain.isNullableDiscreteSet()
                    && domain.getValues().getRanges().getRangeCount() > 1; // Bloom filter is not faster to evaluate for single value
        }
        return false;
    }

    public static Supplier<FilterEvaluator> createBloomFilterEvaluator(Domain domain, int inputChannel)
    {
        return () -> new ColumnarFilterEvaluator(
                new DictionaryAwareColumnarFilter(
                        createColumnarBloomFilter(domain.getType(), inputChannel, domain.getNullableDiscreteSet()).get()));
    }

    private static Supplier<ColumnarFilter> createColumnarBloomFilter(Type type, int inputChannel, Domain.DiscreteSet discreteSet)
    {
        MethodHandle filterConstructor = uncheckedCacheGet(
                COLUMNAR_BLOOM_FILTER_CACHE,
                type,
                () -> generateColumnarBloomFilterClass(type));

        return () -> {
            try {
                SliceBloomFilter filter = new SliceBloomFilter((List<Slice>) (List<?>) discreteSet.getNonNullValues(), discreteSet.containsNull(), type);
                InputChannels inputChannels = new InputChannels(ImmutableList.of(inputChannel), ImmutableList.of(inputChannel));
                return (ColumnarFilter) filterConstructor.invoke(filter, inputChannels);
            }
            catch (Throwable e) {
                throw new RuntimeException(e);
            }
        };
    }

    private static MethodHandle generateColumnarBloomFilterClass(Type type)
    {
        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(ColumnarFilter.class.getSimpleName() + "_bloom_filter_" + type, Optional.empty()),
                type(Object.class),
                type(ColumnarFilter.class));

        FieldDefinition filterField = classDefinition.declareField(a(PRIVATE, FINAL), "filter", SliceBloomFilter.class);
        FieldDefinition inputChannelsField = classDefinition.declareField(a(PRIVATE, FINAL), "inputChannels", InputChannels.class);
        Parameter filterParameter = arg("filter", SliceBloomFilter.class);
        Parameter inputChannelsParameter = arg("inputChannels", InputChannels.class);
        MethodDefinition constructorDefinition = classDefinition.declareConstructor(a(PUBLIC), filterParameter, inputChannelsParameter);
        BytecodeBlock body = constructorDefinition.getBody();
        Variable thisVariable = constructorDefinition.getThis();
        body.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class)
                .append(thisVariable.setField(filterField, filterParameter))
                .append(thisVariable.setField(inputChannelsField, inputChannelsParameter))
                .ret();

        // getInputChannels
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), "getInputChannels", type(InputChannels.class));
        method.getBody().append(method.getScope().getThis().getField(inputChannelsField).ret());

        generateFilterRangeMethod(classDefinition);
        generateFilterListMethod(classDefinition);

        Class<? extends ColumnarFilter> filterClass;
        try {
            filterClass = defineClass(classDefinition, ColumnarFilter.class, ImmutableMap.of(), ColumnarFilterCompiler.class.getClassLoader());
        }
        catch (Exception e) {
            if (Throwables.getRootCause(e) instanceof MethodTooLargeException) {
                throw new TrinoException(QUERY_EXCEEDED_COMPILER_LIMIT,
                        "Query exceeded maximum filters. Please reduce the number of filters referenced and re-run the query.", e);
            }
            throw new TrinoException(COMPILER_ERROR, e.getCause());
        }
        return constructorMethodHandle(filterClass, SliceBloomFilter.class, InputChannels.class);
    }

    private static void generateFilterRangeMethod(ClassDefinition classDefinition)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter outputPositions = arg("outputPositions", int[].class);
        Parameter offset = arg("offset", int.class);
        Parameter size = arg("size", int.class);
        Parameter page = arg("page", Page.class);

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "filterPositionsRange",
                type(int.class),
                ImmutableList.of(session, outputPositions, offset, size, page));
        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        Variable block = declareBlockVariable(page, scope, body);
        Variable outputPositionsCount = scope.declareVariable("outputPositionsCount", body, constantInt(0));
        Variable position = scope.declareVariable(int.class, "position");
        Variable result = scope.declareVariable(boolean.class, "result");

        /* for(int position = offset; position < offset + size; ++position) {
         *   boolean result = block.isNull(position) ? this.filter.containsNull() : this.filter.test(block, position);
         *   outputPositions[outputPositionsCount] = position;
         *   outputPositionsCount += result ? 1 : 0;
         * }
         */
        body.append(new ForLoop("nullable range based loop")
                .initialize(position.set(offset))
                .condition(lessThan(position, add(offset, size)))
                .update(position.increment())
                .body(new BytecodeBlock()
                                .append(generateBloomFilterTest(scope, block, position, result))
                                .append(updateOutputPositions(result, position, outputPositions, outputPositionsCount))));

        body.append(outputPositionsCount.ret());
    }

    private static void generateFilterListMethod(ClassDefinition classDefinition)
    {
        Parameter session = arg("session", ConnectorSession.class);
        Parameter outputPositions = arg("outputPositions", int[].class);
        Parameter activePositions = arg("activePositions", int[].class);
        Parameter offset = arg("offset", int.class);
        Parameter size = arg("size", int.class);
        Parameter page = arg("page", Page.class);

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "filterPositionsList",
                type(int.class),
                ImmutableList.of(session, outputPositions, activePositions, offset, size, page));
        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        Variable block = declareBlockVariable(page, scope, body);
        Variable outputPositionsCount = scope.declareVariable("outputPositionsCount", body, constantInt(0));
        Variable index = scope.declareVariable(int.class, "index");
        Variable position = scope.declareVariable(int.class, "position");
        Variable result = scope.declareVariable(boolean.class, "result");

        /* for(int index = offset; index < offset + size; ++index) {
         *   int position = activePositions[index];
         *   boolean result = block.isNull(position) ? this.filter.containsNull() : this.filter.contains(block, position);
         *   outputPositions[outputPositionsCount] = position;
         *   outputPositionsCount += result ? 1 : 0;
         * }
         */
        body.append(new ForLoop("nullable range based loop")
                .initialize(index.set(offset))
                .condition(lessThan(index, add(offset, size)))
                .update(index.increment())
                .body(new BytecodeBlock()
                        .append(position.set(activePositions.getElement(index)))
                        .append(generateBloomFilterTest(scope, block, position, result))
                        .append(updateOutputPositions(result, position, outputPositions, outputPositionsCount))));

        body.append(outputPositionsCount.ret());
    }

    private static Variable declareBlockVariable(Parameter page, Scope scope, BytecodeBlock body)
    {
        return scope.declareVariable(
                "block",
                body,
                page.invoke("getBlock", Block.class, constantInt(0)));
    }

    private static BytecodeBlock generateBloomFilterTest(Scope scope, Variable block, Variable position, Variable result)
    {
        BytecodeExpression filter = scope.getThis().getField("filter", SliceBloomFilter.class);
        // boolean result = block.isNull(position) ? this.filter.containsNull() : this.filter.contains(block, position)
        return new BytecodeBlock()
                .append(result.set(inlineIf(
                        block.invoke("isNull", boolean.class, position),
                        filter.invoke("containsNull", boolean.class),
                        filter.invoke("contains", boolean.class, block, position))));
    }
}
