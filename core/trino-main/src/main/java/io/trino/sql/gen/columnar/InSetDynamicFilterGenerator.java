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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.FieldDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.operator.project.InputChannels;
import io.trino.spi.type.Type;
import io.trino.sql.gen.CallSiteBinder;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.In;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.lang.invoke.MethodHandle;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.HASH_CODE;
import static io.trino.sql.gen.SqlTypeBytecodeExpression.constantType;
import static io.trino.sql.gen.columnar.ColumnarFilterCompiler.createClassInstance;
import static io.trino.sql.gen.columnar.ColumnarFilterCompiler.generateGetInputChannels;
import static io.trino.sql.gen.columnar.InColumnarFilterGenerator.generateInFilterListMethod;
import static io.trino.sql.gen.columnar.InColumnarFilterGenerator.generateInFilterRangeMethod;
import static io.trino.util.CompilerUtils.makeClassName;
import static io.trino.util.FastutilSetHelper.toFastutilHashSet;
import static java.util.Objects.requireNonNull;

// Generates an IN filter that reads its value set from an instance field, so one class per
// (value type, set class) is reused across dynamic filters with a stable JVM class identity.
public final class InSetDynamicFilterGenerator
{
    private final Reference valueReference;
    private final Map<Symbol, Integer> layout;
    private final Type valueType;
    private final Class<? extends LongSet> setClass;
    private final LongSet valueSet;
    private final int valueChannel;

    // Returns empty when the IN predicate is not an eligible long-backed dynamic filter, so the caller
    // falls back to the shared cache path.
    public static Optional<InSetDynamicFilterGenerator> tryCreate(In in, Map<Symbol, Integer> layout, Metadata metadata, FunctionManager functionManager)
    {
        if (!(in.value() instanceof Reference valueReference) || valueReference.type().getJavaType() != long.class) {
            return Optional.empty();
        }
        ImmutableSet.Builder<Object> valuesBuilder = ImmutableSet.builder();
        for (Expression expression : in.valueList()) {
            if (!(expression instanceof Constant constant) || constant.value() == null) {
                return Optional.empty();
            }
            valuesBuilder.add(constant.value());
        }
        Set<Object> values = valuesBuilder.build();
        if (values.isEmpty()) {
            return Optional.empty();
        }

        Type valueType = valueReference.type();
        // Small integer IN lists compile to a lookupswitch with tiny baked data; leave them on the
        // shared cache path where the switch is faster and retention is not a concern.
        if (InColumnarFilterGenerator.useSwitchCaseGeneration(valueType, in.valueList())) {
            return Optional.empty();
        }
        ResolvedFunction resolvedEquals = metadata.resolveOperator(EQUAL, ImmutableList.of(valueType, valueType));
        ResolvedFunction resolvedHashCode = metadata.resolveOperator(HASH_CODE, ImmutableList.of(valueType));
        MethodHandle equalsHandle = functionManager.getScalarFunctionImplementation(resolvedEquals, simpleConvention(NULLABLE_RETURN, NEVER_NULL, NEVER_NULL)).getMethodHandle();
        MethodHandle hashCodeHandle = functionManager.getScalarFunctionImplementation(resolvedHashCode, simpleConvention(FAIL_ON_NULL, NEVER_NULL)).getMethodHandle();
        LongSet valueSet = (LongSet) toFastutilHashSet(values, valueType, hashCodeHandle, equalsHandle);
        return Optional.of(new InSetDynamicFilterGenerator(valueReference, layout, valueSet));
    }

    private InSetDynamicFilterGenerator(Reference valueReference, Map<Symbol, Integer> layout, LongSet valueSet)
    {
        this.valueReference = requireNonNull(valueReference, "valueReference is null");
        this.layout = requireNonNull(layout, "layout is null");
        this.valueSet = requireNonNull(valueSet, "valueSet is null");
        this.valueType = valueReference.type();
        this.setClass = valueSet.getClass().asSubclass(LongSet.class);
        Integer channel = layout.get(Symbol.from(valueReference));
        checkState(channel != null, "Reference not in layout: %s", valueReference.name());
        valueChannel = channel;
    }

    public Type valueType()
    {
        return valueType;
    }

    public Class<? extends LongSet> setClass()
    {
        return setClass;
    }

    public LongSet valueSet()
    {
        return valueSet;
    }

    public Class<? extends ColumnarFilter> generateColumnarFilter()
    {
        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(ColumnarFilter.class.getSimpleName() + "_in_dynamic", Optional.empty()),
                type(Object.class),
                type(ColumnarFilter.class));
        CallSiteBinder callSiteBinder = new CallSiteBinder();

        FieldDefinition inputChannelsField = generateGetInputChannels(classDefinition);
        FieldDefinition valueSetField = classDefinition.declareField(a(PRIVATE, FINAL), "valueSet", setClass);
        generateConstructor(classDefinition, inputChannelsField, valueSetField, setClass);

        generateInFilterRangeMethod(
                classDefinition,
                valueReference,
                layout,
                (scope, position, result) -> generateSetContainsCall(callSiteBinder, valueSetField, scope, position, result));
        generateInFilterListMethod(
                classDefinition,
                valueReference,
                layout,
                (scope, position, result) -> generateSetContainsCall(callSiteBinder, valueSetField, scope, position, result));

        return createClassInstance(callSiteBinder, classDefinition);
    }

    private static void generateConstructor(ClassDefinition classDefinition, FieldDefinition inputChannelsField, FieldDefinition valueSetField, Class<? extends LongSet> setClass)
    {
        Parameter inputChannelsParam = arg("inputChannels", InputChannels.class);
        Parameter valueSetParam = arg("valueSet", setClass);
        MethodDefinition constructorDefinition = classDefinition.declareConstructor(a(PUBLIC), inputChannelsParam, valueSetParam);

        BytecodeBlock body = constructorDefinition.getBody();
        Variable thisVariable = constructorDefinition.getThis();

        body.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);
        body.append(thisVariable.setField(inputChannelsField, inputChannelsParam));
        body.append(thisVariable.setField(valueSetField, valueSetParam));
        body.ret();
    }

    private BytecodeBlock generateSetContainsCall(CallSiteBinder binder, FieldDefinition valueSetField, Scope scope, BytecodeExpression position, Variable result)
    {
        Type valueType = valueReference.type();
        BytecodeExpression value = constantType(binder, valueType)
                .invoke("getLong", long.class, scope.getVariable("block_" + valueChannel), position);
        return new BytecodeBlock()
                .comment("valueSet.contains(<stackValue>)")
                .append(result.set(scope.getThis()
                        .getField(valueSetField)
                        .invoke("contains", boolean.class, value)));
    }
}
