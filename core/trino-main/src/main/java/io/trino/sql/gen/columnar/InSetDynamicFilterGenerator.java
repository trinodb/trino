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
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.CallSiteBinder;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.In;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.type.CharVarcharCoercion;

import java.lang.invoke.MethodHandle;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
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
import static io.trino.sql.gen.BytecodeUtils.generateToString;
import static io.trino.sql.gen.columnar.ColumnarFilterCompiler.createClassInstanceDirect;
import static io.trino.sql.gen.columnar.ColumnarFilterCompiler.generateGetInputChannels;
import static io.trino.sql.gen.columnar.InColumnarFilterGenerator.MAX_SWITCH_CASE_VALUES;
import static io.trino.sql.gen.columnar.InColumnarFilterGenerator.generateInFilterMethods;
import static io.trino.util.CompilerUtils.makeClassName;
import static io.trino.util.FastutilSetHelper.toFastutilHashSet;
import static java.util.Objects.requireNonNull;

// Generates an IN filter that reads its value set from an instance field, so one class per
// (value type, set class) is reused across dynamic filters with a stable JVM class identity.
public final class InSetDynamicFilterGenerator
{
    private final Reference valueReference;
    private final Map<Symbol, Integer> layout;
    private final TypeOperators typeOperators;
    private final Type valueType;
    private final Class<?> setClass;
    private final Set<?> valueSet;

    // Returns empty when the IN predicate is not an eligible dynamic filter, so the caller
    // falls back to the shared cache path.
    public static Optional<InSetDynamicFilterGenerator> tryCreate(In in, Map<Symbol, Integer> layout, Metadata metadata, CharVarcharCoercion charVarcharCoercion, FunctionManager functionManager, TypeOperators typeOperators)
    {
        if (!(in.value() instanceof Reference valueReference)) {
            return Optional.empty();
        }
        Type valueType = valueReference.type();
        // FastutilSetHelper does not handle indeterminate values in structural types
        if (isStructural(valueType)) {
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

        // Small integer IN lists compile to a lookupswitch with tiny baked data; leave them on the
        // shared cache path where the switch is faster and retention is not a concern.
        if (InColumnarFilterGenerator.useSwitchCaseGeneration(valueType, in.valueList())) {
            return Optional.empty();
        }
        return Optional.of(create(valueReference, values, layout, metadata, charVarcharCoercion, functionManager, typeOperators));
    }

    // Creates the generator straight from the values of a discrete set, without materializing an IN expression.
    // The values must be distinct and not null. Returns empty when the values are not eligible for a dynamic
    // filter, which is the case for the same types and sizes as in tryCreate(In, ...).
    public static Optional<InSetDynamicFilterGenerator> tryCreate(Reference valueReference, List<Object> distinctNonNullValues, Map<Symbol, Integer> layout, Metadata metadata, CharVarcharCoercion charVarcharCoercion, FunctionManager functionManager, TypeOperators typeOperators)
    {
        Type valueType = valueReference.type();
        if (isStructural(valueType) || distinctNonNullValues.isEmpty()) {
            return Optional.empty();
        }
        // The same condition as in tryCreate(In, ...); only lists shorter than the threshold can use a switch,
        // so the expression list is built only for them
        if (distinctNonNullValues.size() < MAX_SWITCH_CASE_VALUES) {
            List<Expression> constants = distinctNonNullValues.stream()
                    .map(value -> (Expression) new Constant(valueType, value))
                    .collect(toImmutableList());
            if (InColumnarFilterGenerator.useSwitchCaseGeneration(valueType, constants)) {
                return Optional.empty();
            }
        }
        return Optional.of(create(valueReference, new DistinctValues(distinctNonNullValues), layout, metadata, charVarcharCoercion, functionManager, typeOperators));
    }

    private static boolean isStructural(Type valueType)
    {
        return valueType instanceof ArrayType || valueType instanceof MapType || valueType instanceof RowType;
    }

    private static InSetDynamicFilterGenerator create(Reference valueReference, Set<Object> values, Map<Symbol, Integer> layout, Metadata metadata, CharVarcharCoercion charVarcharCoercion, FunctionManager functionManager, TypeOperators typeOperators)
    {
        Type valueType = valueReference.type();
        ResolvedFunction resolvedEquals = metadata.resolveOperator(charVarcharCoercion, EQUAL, ImmutableList.of(valueType, valueType));
        ResolvedFunction resolvedHashCode = metadata.resolveOperator(charVarcharCoercion, HASH_CODE, ImmutableList.of(valueType));
        MethodHandle equalsHandle = functionManager.getScalarFunctionImplementation(resolvedEquals, simpleConvention(NULLABLE_RETURN, NEVER_NULL, NEVER_NULL)).getMethodHandle();
        MethodHandle hashCodeHandle = functionManager.getScalarFunctionImplementation(resolvedHashCode, simpleConvention(FAIL_ON_NULL, NEVER_NULL)).getMethodHandle();
        Set<?> valueSet = toFastutilHashSet(values, valueType, hashCodeHandle, equalsHandle);
        return new InSetDynamicFilterGenerator(valueReference, layout, valueSet, typeOperators);
    }

    private InSetDynamicFilterGenerator(Reference valueReference, Map<Symbol, Integer> layout, Set<?> valueSet, TypeOperators typeOperators)
    {
        this.valueReference = requireNonNull(valueReference, "valueReference is null");
        this.layout = requireNonNull(layout, "layout is null");
        this.valueSet = requireNonNull(valueSet, "valueSet is null");
        this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");
        this.valueType = valueReference.type();
        this.setClass = valueSet.getClass();
    }

    public Type valueType()
    {
        return valueType;
    }

    public Class<?> setClass()
    {
        return setClass;
    }

    public Set<?> valueSet()
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

        generateInFilterMethods(
                classDefinition,
                callSiteBinder,
                typeOperators,
                valueReference,
                layout,
                (scope, value, result) -> generateSetContainsCall(valueSetField, scope, value, result));

        // like every other columnar filter class, describe itself in debuggers and logs; the
        // bound value set is not included, since it can be arbitrarily large and this class is
        // shared across dynamic filters that each bind their own set as a constructor argument
        String description = ColumnarFilter.class.getSimpleName() + "{IN dynamic filter, valueType=" + valueType + "}";
        generateToString(classDefinition, callSiteBinder.bind(description, Object.class));

        return createClassInstanceDirect(callSiteBinder, classDefinition);
    }

    private static void generateConstructor(ClassDefinition classDefinition, FieldDefinition inputChannelsField, FieldDefinition valueSetField, Class<?> setClass)
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

    private BytecodeBlock generateSetContainsCall(FieldDefinition valueSetField, Scope scope, BytecodeExpression value, Variable result)
    {
        BytecodeExpression element = value;
        if (!valueType.getJavaType().isPrimitive()) {
            element = value.cast(Object.class);
        }
        return new BytecodeBlock()
                .comment("valueSet.contains(value)")
                .append(result.set(scope.getThis()
                        .getField(valueSetField)
                        .invoke("contains", boolean.class, element)));
    }

    // A read-only view of a list of distinct values as a set, which spares hashing the values
    // that the fastutil set built from them hashes anyway. It is only iterated and measured.
    private static final class DistinctValues
            extends AbstractSet<Object>
    {
        private final List<Object> values;

        private DistinctValues(List<Object> values)
        {
            this.values = requireNonNull(values, "values is null");
        }

        @Override
        public Iterator<Object> iterator()
        {
            return values.iterator();
        }

        @Override
        public int size()
        {
            return values.size();
        }
    }
}
