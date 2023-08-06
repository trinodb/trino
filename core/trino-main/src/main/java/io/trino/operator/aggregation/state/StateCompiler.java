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
package io.trino.operator.aggregation.state;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.FieldDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.IfStatement;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.trino.array.BlockBigArray;
import io.trino.array.BooleanBigArray;
import io.trino.array.ByteBigArray;
import io.trino.array.DoubleBigArray;
import io.trino.array.IntBigArray;
import io.trino.array.LongBigArray;
import io.trino.array.ObjectBigArray;
import io.trino.array.SliceBigArray;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.RowValueBuilder;
import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.AccumulatorStateMetadata;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.function.GroupedAccumulatorState;
import io.trino.spi.function.InOut;
import io.trino.spi.function.InternalDataAccessor;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.gen.ClassBuilder;
import io.trino.sql.gen.SqlTypeBytecodeExpression;

import java.lang.annotation.Annotation;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.UPPER_CAMEL;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.STATIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.add;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantBoolean;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantClass;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantFalse;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantInt;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantLong;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantNull;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantNumber;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantString;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantTrue;
import static io.airlift.bytecode.expression.BytecodeExpressions.defaultValue;
import static io.airlift.bytecode.expression.BytecodeExpressions.equal;
import static io.airlift.bytecode.expression.BytecodeExpressions.getStatic;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeStatic;
import static io.airlift.bytecode.expression.BytecodeExpressions.isNull;
import static io.airlift.bytecode.expression.BytecodeExpressions.newInstance;
import static io.airlift.bytecode.expression.BytecodeExpressions.setStatic;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.sql.gen.LambdaMetafactoryGenerator.generateMetafactory;
import static io.trino.sql.gen.SqlTypeBytecodeExpression.constantType;
import static io.trino.type.UnknownType.UNKNOWN;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodHandles.privateLookupIn;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Objects.requireNonNull;

public final class StateCompiler
{
    private StateCompiler() {}

    public static <T extends AccumulatorState> AccumulatorStateSerializer<T> generateStateSerializer(Class<T> clazz)
    {
        return generateStateSerializer(clazz, ImmutableMap.of());
    }

    @VisibleForTesting
    static <T extends AccumulatorState> AccumulatorStateSerializer<T> generateStateSerializer(Class<T> clazz, Map<String, Type> fieldTypes)
    {
        AccumulatorStateMetadata metadata = getMetadataAnnotation(clazz);
        if (metadata != null && metadata.stateSerializerClass() != AccumulatorStateSerializer.class) {
            try {
                //noinspection unchecked
                return (AccumulatorStateSerializer<T>) metadata.stateSerializerClass().getConstructor().newInstance();
            }
            catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }

        ClassBuilder classBuilder = ClassBuilder.createStandardClass(
                lookup(),
                a(PUBLIC, FINAL),
                clazz.getSimpleName() + "Serializer",
                type(Object.class),
                type(AccumulatorStateSerializer.class));

        // Generate constructor
        classBuilder.declareDefaultConstructor(a(PUBLIC));

        List<StateField> fields = enumerateFields(clazz, fieldTypes);
        generateGetSerializedType(classBuilder, fields);
        generateSerialize(classBuilder, clazz, fields);
        generateDeserialize(classBuilder, clazz, fields);

        Class<?> serializerClass = classBuilder.defineClass(AccumulatorStateSerializer.class);
        try {
            //noinspection unchecked
            return (AccumulatorStateSerializer<T>) serializerClass.getConstructor().newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static void generateGetSerializedType(ClassBuilder classBuilder, List<StateField> fields)
    {
        BytecodeBlock body = classBuilder.declareMethod(a(PUBLIC), "getSerializedType", type(Type.class)).getBody();

        Type type = getSerializedType(fields);

        body.comment("return %s", type.getTypeSignature())
                .append(constantType(classBuilder, type))
                .retObject();
    }

    private static Type getSerializedType(List<StateField> fields)
    {
        if (fields.size() > 1) {
            List<Type> types = fields.stream().map(StateField::sqlType).collect(toImmutableList());
            return RowType.anonymous(types);
        }
        if (fields.size() == 1) {
            return getOnlyElement(fields).sqlType();
        }
        return UNKNOWN;
    }

    public static <T> AccumulatorStateMetadata getMetadataAnnotation(Class<T> clazz)
    {
        AccumulatorStateMetadata metadata = clazz.getAnnotation(AccumulatorStateMetadata.class);
        if (metadata != null) {
            return metadata;
        }
        // If the annotation wasn't found, then search the super classes
        for (Class<?> superInterface : clazz.getInterfaces()) {
            metadata = superInterface.getAnnotation(AccumulatorStateMetadata.class);
            if (metadata != null) {
                return metadata;
            }
        }

        return null;
    }

    private static <T extends AccumulatorState> void generateDeserialize(ClassBuilder classBuilder, Class<T> clazz, List<StateField> fields)
    {
        Parameter block = arg("block", Block.class);
        Parameter index = arg("index", int.class);
        Parameter state = arg("state", AccumulatorState.class);
        MethodDefinition method = classBuilder.declareMethod(a(PUBLIC), "deserialize", type(void.class), block, index, state);
        BytecodeBlock deserializerBody = method.getBody();
        Scope scope = method.getScope();
        if (fields.size() == 1) {
            StateField field = getOnlyElement(fields);
            Method setter = getSetter(clazz, field);
            if (!field.isPrimitiveType()) {
                deserializerBody.append(new IfStatement()
                        .condition(block.invoke("isNull", boolean.class, index))
                        .ifTrue(state.cast(setter.getDeclaringClass()).invoke(setter, constantNull(field.type())))
                        .ifFalse(state.cast(setter.getDeclaringClass()).invoke(setter, constantType(classBuilder, field.sqlType()).getValue(block, index))));
            }
            else {
                // For primitive type, we need to cast here because we serialize byte fields with TINYINT/INTEGER (whose java type is long).
                deserializerBody.append(
                        state.cast(setter.getDeclaringClass()).invoke(
                                setter,
                                constantType(classBuilder, field.sqlType()).getValue(block, index).cast(field.type())));
            }
        }
        else if (fields.size() > 1) {
            Variable row = scope.declareVariable(Block.class, "row");
            deserializerBody.append(row.set(block.invoke("getObject", Object.class, index, constantClass(Block.class)).cast(Block.class)));
            int position = 0;
            for (StateField field : fields) {
                Method setter = getSetter(clazz, field);
                if (!field.isPrimitiveType()) {
                    deserializerBody.append(new IfStatement()
                            .condition(row.invoke("isNull", boolean.class, constantInt(position)))
                            .ifTrue(state.cast(setter.getDeclaringClass()).invoke(setter, constantNull(field.type())))
                            .ifFalse(state.cast(setter.getDeclaringClass()).invoke(setter, constantType(classBuilder, field.sqlType()).getValue(row, constantInt(position)))));
                }
                else {
                    // For primitive type, we need to cast here because we serialize byte fields with TINYINT/INTEGER (whose java type is long).
                    deserializerBody.append(
                            state.cast(setter.getDeclaringClass()).invoke(
                                    setter,
                                    constantType(classBuilder, field.sqlType()).getValue(row, constantInt(position)).cast(field.type())));
                }
                position++;
            }
        }
        deserializerBody.ret();
    }

    private static <T> void generateSerialize(ClassBuilder classBuilder, Class<T> clazz, List<StateField> fields)
    {
        Parameter state = arg("state", AccumulatorState.class);
        Parameter out = arg("out", BlockBuilder.class);
        MethodDefinition method = classBuilder.declareMethod(a(PUBLIC), "serialize", type(void.class), state, out);
        Scope scope = method.getScope();
        BytecodeBlock serializerBody = method.getBody();

        if (fields.isEmpty()) {
            serializerBody.append(out.invoke("appendNull", BlockBuilder.class).pop());
        }
        else if (fields.size() == 1) {
            Method getter = getGetter(clazz, getOnlyElement(fields));
            SqlTypeBytecodeExpression sqlType = constantType(classBuilder, getOnlyElement(fields).sqlType());
            Variable fieldValue = scope.declareVariable(getter.getReturnType(), "value");
            serializerBody.append(fieldValue.set(state.cast(getter.getDeclaringClass()).invoke(getter)));
            if (!getOnlyElement(fields).isPrimitiveType()) {
                serializerBody.append(new IfStatement()
                        .condition(equal(fieldValue, constantNull(getter.getReturnType())))
                        .ifTrue(out.invoke("appendNull", BlockBuilder.class).pop())
                        .ifFalse(sqlType.writeValue(out, fieldValue)));
            }
            else {
                // For primitive type, we need to cast here because we serialize byte fields with TINYINT/INTEGER (whose java type is long).
                serializerBody.append(sqlType.writeValue(out, fieldValue.cast(getOnlyElement(fields).sqlType().getJavaType())));
            }
        }
        else {
            MethodDefinition serializeToRow = generateSerializeToRow(classBuilder, clazz, fields);
            BytecodeExpression rowEntryBuilder = generateMetafactory(RowValueBuilder.class, serializeToRow, ImmutableList.of(state));
            serializerBody.append(out.cast(RowBlockBuilder.class).invoke("buildEntry", void.class, rowEntryBuilder));
        }
        serializerBody.ret();
    }

    private static MethodDefinition generateSerializeToRow(
            ClassBuilder classBuilder,
            Class<?> clazz,
            List<StateField> fields)
    {
        Parameter state = arg("state", AccumulatorState.class);
        Parameter fieldBuilders = arg("fieldBuilders", type(List.class, BlockBuilder.class));
        MethodDefinition method = classBuilder.declareMethod(a(PRIVATE, STATIC), "serialize", type(void.class), state, fieldBuilders);
        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        Variable fieldBuilder = scope.createTempVariable(BlockBuilder.class);
        for (int i = 0; i < fields.size(); i++) {
            StateField field = fields.get(i);
            Method getter = getGetter(clazz, field);

            SqlTypeBytecodeExpression sqlType = constantType(classBuilder, field.sqlType());

            Variable fieldValue = scope.createTempVariable(getter.getReturnType());
            body.append(fieldValue.set(state.cast(getter.getDeclaringClass()).invoke(getter)));

            body.append(fieldBuilder.set(fieldBuilders.invoke("get", Object.class, constantInt(i)).cast(BlockBuilder.class)));

            if (!field.isPrimitiveType()) {
                body.append(new IfStatement().condition(equal(fieldValue, constantNull(getter.getReturnType())))
                        .ifTrue(fieldBuilder.invoke("appendNull", BlockBuilder.class).pop())
                        .ifFalse(sqlType.writeValue(fieldBuilder, fieldValue)));
            }
            else {
                // For primitive type, we need to cast here because we serialize byte fields with TINYINT/INTEGER (whose java type is long).
                body.append(sqlType.writeValue(fieldBuilder, fieldValue.cast(field.sqlType().getJavaType())));
            }
        }
        body.ret();
        return method;
    }

    private static Method getSetter(Class<?> clazz, StateField field)
    {
        try {
            return clazz.getMethod(field.setterName(), field.type());
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private static Method getGetter(Class<?> clazz, StateField field)
    {
        try {
            return clazz.getMethod(field.getterName());
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    public static AccumulatorStateFactory<InOut> generateInOutStateFactory(Type type)
    {
        Class<? extends InOut> singleStateClass = generateInOutSingleStateClass(type);
        Class<? extends InOut> groupedStateClass = generateInOutGroupedStateClass(type);
        return generateStateFactory(InOut.class, singleStateClass, groupedStateClass);
    }

    private static Class<? extends InOut> generateInOutSingleStateClass(Type type)
    {
        ClassBuilder classBuilder = ClassBuilder.createHiddenClass(
                lookup(),
                a(PUBLIC, FINAL),
                "SingleInOut",
                type(Object.class),
                type(InOut.class),
                type(InternalDataAccessor.class));

        estimatedSize(classBuilder);

        // Generate constructor
        MethodDefinition constructor = classBuilder.declareConstructor(a(PUBLIC));

        constructor.getBody()
                .append(constructor.getThis())
                .invokeConstructor(Object.class);

        // Generate fields
        FieldDefinition valueField = classBuilder.declareField(a(PRIVATE), "value", inOutGetterReturnType(type));
        Function<Scope, BytecodeExpression> valueGetter = scope -> scope.getThis().getField(valueField);

        Optional<FieldDefinition> nullField;
        Function<Scope, BytecodeExpression> nullGetter;
        if (type.getJavaType().isPrimitive()) {
            nullField = Optional.of(classBuilder.declareField(a(PRIVATE), "valueIdNull", boolean.class));
            constructor.getBody().append(constructor.getThis().setField(nullField.get(), constantTrue()));
            nullGetter = scope -> scope.getThis().getField(nullField.get());
        }
        else {
            nullField = Optional.empty();
            nullGetter = scope -> isNull(valueGetter.apply(scope));
        }

        constructor.getBody()
                .ret();

        inOutSingleCopy(classBuilder, valueField, nullField);

        Function<Scope, BytecodeNode> setNullGenerator = scope -> {
            Variable thisVariable = scope.getThis();
            BytecodeBlock bytecodeBlock = new BytecodeBlock();
            nullField.ifPresent(field -> bytecodeBlock.append(thisVariable.setField(field, constantTrue())));
            bytecodeBlock.append(thisVariable.setField(valueField, defaultValue(valueField.getType())));
            return bytecodeBlock;
        };

        BiFunction<Scope, BytecodeExpression, BytecodeNode> setValueGenerator = (scope, value) -> {
            Variable thisVariable = scope.getThis();
            BytecodeBlock bytecodeBlock = new BytecodeBlock();
            nullField.ifPresent(field -> bytecodeBlock.append(thisVariable.setField(field, constantFalse())));
            bytecodeBlock.append(thisVariable.setField(valueField, value));
            return bytecodeBlock;
        };

        generateInOutMethods(type, classBuilder, valueGetter, nullGetter, setNullGenerator, setValueGenerator);
        return classBuilder.defineClass(InOut.class);
    }

    private static Class<? extends InOut> generateInOutGroupedStateClass(Type type)
    {
        ClassBuilder classBuilder = ClassBuilder.createHiddenClass(
                lookup(),
                a(PUBLIC, FINAL),
                "GroupedInOut", // todo add type
                type(Object.class),
                type(InOut.class),
                type(GroupedAccumulatorState.class),
                type(InternalDataAccessor.class));

        estimatedSize(classBuilder);

        MethodDefinition constructor = classBuilder.declareConstructor(a(PUBLIC));
        constructor.getBody()
                .append(constructor.getThis())
                .invokeConstructor(Object.class);

        FieldDefinition groupIdField = classBuilder.declareField(a(PRIVATE), "groupId", long.class);

        Class<?> valueElementType = inOutGetterReturnType(type);
        FieldDefinition valueField = classBuilder.declareField(a(PRIVATE, FINAL), "value", getBigArrayType(valueElementType).getBigArrayType());
        constructor.getBody().append(constructor.getThis().setField(valueField, newInstance(valueField.getType())));
        Function<Scope, BytecodeExpression> valueGetter = scope -> scope.getThis().getField(valueField).invoke("get", valueElementType, scope.getThis().getField(groupIdField));

        Optional<FieldDefinition> nullField;
        Function<Scope, BytecodeExpression> nullGetter;
        if (type.getJavaType().isPrimitive()) {
            nullField = Optional.of(classBuilder.declareField(a(PRIVATE, FINAL), "valueIdNull", BooleanBigArray.class));
            constructor.getBody().append(constructor.getThis().setField(nullField.get(), newInstance(BooleanBigArray.class, constantTrue())));
            nullGetter = scope -> scope.getThis().getField(nullField.get()).invoke("get", boolean.class, scope.getThis().getField(groupIdField));
        }
        else {
            nullField = Optional.empty();
            nullGetter = scope -> isNull(valueGetter.apply(scope));
        }

        constructor.getBody()
                .ret();

        inOutGroupedSetGroupId(classBuilder, groupIdField);
        inOutGroupedEnsureCapacity(classBuilder, valueField, nullField);
        inOutGroupedCopy(classBuilder, valueField, nullField);

        Function<Scope, BytecodeNode> setNullGenerator = scope -> {
            Variable thisVariable = scope.getThis();
            BytecodeBlock bytecodeBlock = new BytecodeBlock();
            nullField.ifPresent(field -> bytecodeBlock.append(thisVariable.getField(field).invoke("set", void.class, thisVariable.getField(groupIdField), constantTrue())));
            bytecodeBlock.append(thisVariable.getField(valueField).invoke("set", void.class, thisVariable.getField(groupIdField), defaultValue(valueElementType)));
            return bytecodeBlock;
        };
        BiFunction<Scope, BytecodeExpression, BytecodeNode> setValueGenerator = (scope, value) -> {
            Variable thisVariable = scope.getThis();
            BytecodeBlock bytecodeBlock = new BytecodeBlock();
            nullField.ifPresent(field -> bytecodeBlock.append(thisVariable.getField(field).invoke("set", void.class, thisVariable.getField(groupIdField), constantFalse())));
            bytecodeBlock.append(thisVariable.getField(valueField).invoke("set", void.class, thisVariable.getField(groupIdField), value.cast(valueElementType)));
            return bytecodeBlock;
        };

        generateInOutMethods(type, classBuilder, valueGetter, nullGetter, setNullGenerator, setValueGenerator);

        return classBuilder.defineClass(InOut.class);
    }

    private static void generateInOutMethods(Type type,
            ClassBuilder classBuilder,
            Function<Scope, BytecodeExpression> valueGetter,
            Function<Scope, BytecodeExpression> nullGetter,
            Function<Scope, BytecodeNode> setNullGenerator,
            BiFunction<Scope, BytecodeExpression, BytecodeNode> setValueGenerator)
    {
        SqlTypeBytecodeExpression sqlType = constantType(classBuilder, type);

        generateInOutGetType(classBuilder, sqlType);
        generateInOutIsNull(classBuilder, nullGetter);
        generateInOutGetBlockBuilder(classBuilder, sqlType, valueGetter);
        generateInOutSetBlockPosition(classBuilder, sqlType, setNullGenerator, setValueGenerator);
        generateInOutSetInOut(classBuilder, type, setNullGenerator, setValueGenerator);
        generateInOutGetValue(classBuilder, type, valueGetter);
    }

    private static void estimatedSize(ClassBuilder classBuilder)
    {
        FieldDefinition instanceSize = generateInstanceSize(classBuilder);

        // Add getter for class size
        classBuilder.declareMethod(a(PUBLIC), "getEstimatedSize", type(long.class))
                .getBody()
                .getStaticField(instanceSize)
                .retLong();
    }

    private static void inOutSingleCopy(ClassBuilder classBuilder, FieldDefinition valueField, Optional<FieldDefinition> nullField)
    {
        MethodDefinition copy = classBuilder.declareMethod(a(PUBLIC), "copy", type(AccumulatorState.class));
        Variable thisVariable = copy.getThis();
        BytecodeBlock body = copy.getBody();

        Variable copyVariable = copy.getScope().declareVariable(classBuilder.getType(), "copy");
        body.append(copyVariable.set(newInstance(classBuilder.getType())));
        body.append(copyVariable.setField(valueField, thisVariable.getField(valueField)));
        nullField.ifPresent(field -> body.append(copyVariable.setField(field, thisVariable.getField(field))));
        body.append(copyVariable.ret());
    }

    private static void inOutGroupedSetGroupId(ClassBuilder classBuilder, FieldDefinition groupIdField)
    {
        Parameter groupIdArg = arg("groupId", long.class);
        MethodDefinition method = classBuilder.declareMethod(a(PUBLIC), "setGroupId", type(void.class), groupIdArg);
        method.getBody()
                .append(method.getThis().setField(groupIdField, groupIdArg))
                .ret();
    }

    private static void inOutGroupedEnsureCapacity(ClassBuilder classBuilder, FieldDefinition valueField, Optional<FieldDefinition> nullField)
    {
        Parameter size = arg("size", long.class);
        MethodDefinition method = classBuilder.declareMethod(a(PUBLIC), "ensureCapacity", type(void.class), size);
        Variable thisVariable = method.getThis();
        BytecodeBlock body = method.getBody();

        body.append(thisVariable.getField(valueField).invoke("ensureCapacity", void.class, size));
        nullField.ifPresent(field -> body.append(thisVariable.getField(field).invoke("ensureCapacity", void.class, size)));
        body.ret();
    }

    private static void inOutGroupedCopy(ClassBuilder classBuilder, FieldDefinition valueField, Optional<FieldDefinition> nullField)
    {
        MethodDefinition copy = classBuilder.declareMethod(a(PUBLIC), "copy", type(AccumulatorState.class));
        Variable thisVariable = copy.getThis();
        BytecodeBlock body = copy.getBody();

        Variable copyVariable = copy.getScope().declareVariable(classBuilder.getType(), "copy");
        body.append(copyVariable.set(newInstance(classBuilder.getType())));
        copyBigArray(body, thisVariable, copyVariable, valueField);
        nullField.ifPresent(field -> copyBigArray(body, thisVariable, copyVariable, field));
        body.append(copyVariable.ret());
    }

    private static void copyBigArray(BytecodeBlock body, Variable source, Variable destination, FieldDefinition bigArrayField)
    {
        body.append(destination.getField(bigArrayField).invoke("ensureCapacity", void.class, source.getField(bigArrayField).invoke("getCapacity", long.class)));
        body.append(source.getField(bigArrayField).invoke(
                "copyTo",
                void.class,
                constantLong(0),
                destination.getField(bigArrayField),
                constantLong(0),
                source.getField(bigArrayField).invoke("getCapacity", long.class)));
    }

    private static void generateInOutGetType(ClassBuilder classBuilder, SqlTypeBytecodeExpression sqlType)
    {
        classBuilder.declareMethod(a(PUBLIC), "getType", type(Type.class))
                .getBody()
                .append(sqlType.ret());
    }

    private static void generateInOutIsNull(ClassBuilder classBuilder, Function<Scope, BytecodeExpression> nullGetter)
    {
        MethodDefinition isNullMethod = classBuilder.declareMethod(a(PUBLIC), "isNull", type(boolean.class));
        isNullMethod.getBody().append(nullGetter.apply(isNullMethod.getScope()).ret());
    }

    private static void generateInOutGetBlockBuilder(ClassBuilder classBuilder, SqlTypeBytecodeExpression sqlType, Function<Scope, BytecodeExpression> valueGetter)
    {
        Parameter blockBuilderArg = arg("blockBuilder", BlockBuilder.class);
        MethodDefinition getBlockBuilderMethod = classBuilder.declareMethod(a(PUBLIC), "get", type(void.class), blockBuilderArg);
        Variable thisVariable = getBlockBuilderMethod.getThis();
        BytecodeBlock body = getBlockBuilderMethod.getBody();

        body.append(new IfStatement()
                .condition(thisVariable.invoke("isNull", boolean.class))
                .ifTrue(blockBuilderArg.invoke("appendNull", BlockBuilder.class).pop())
                .ifFalse(sqlType.writeValue(blockBuilderArg, valueGetter.apply(getBlockBuilderMethod.getScope()))));
        body.ret();
    }

    private static void generateInOutSetBlockPosition(
            ClassBuilder classBuilder,
            SqlTypeBytecodeExpression sqlType,
            Function<Scope, BytecodeNode> setNullGenerator,
            BiFunction<Scope, BytecodeExpression, BytecodeNode> setValueGenerator)
    {
        Parameter blockArg = arg("block", Block.class);
        Parameter positionArg = arg("position", int.class);
        MethodDefinition setBlockBuilderMethod = classBuilder.declareMethod(a(PUBLIC), "set", type(void.class), blockArg, positionArg);
        BytecodeBlock body = setBlockBuilderMethod.getBody();

        body.append(new IfStatement()
                .condition(blockArg.invoke("isNull", boolean.class, positionArg))
                .ifTrue(setNullGenerator.apply(setBlockBuilderMethod.getScope()))
                .ifFalse(setValueGenerator.apply(setBlockBuilderMethod.getScope(), sqlType.getValue(blockArg, positionArg))));
        body.ret();
    }

    private static void generateInOutSetInOut(
            ClassBuilder classBuilder,
            Type type,
            Function<Scope, BytecodeNode> setNullGenerator,
            BiFunction<Scope, BytecodeExpression, BytecodeNode> setValueGenerator)
    {
        Parameter otherState = arg("otherState", InOut.class);
        MethodDefinition setter = classBuilder.declareMethod(a(PUBLIC), "set", type(void.class), otherState);
        BytecodeBlock body = setter.getBody();

        body.append(new IfStatement()
                .condition(otherState.invoke("isNull", boolean.class))
                .ifTrue(setNullGenerator.apply(setter.getScope()))
                .ifFalse(setValueGenerator.apply(setter.getScope(), otherState.cast(InternalDataAccessor.class).invoke(inOutGetterName(type), inOutGetterReturnType(type)))));
        body.ret();
    }

    private static void generateInOutGetValue(ClassBuilder classBuilder, Type type, Function<Scope, BytecodeExpression> valueGetter)
    {
        MethodDefinition getter = classBuilder.declareMethod(a(PUBLIC), inOutGetterName(type), type(inOutGetterReturnType(type)));
        getter.getBody().append(valueGetter.apply(getter.getScope()).ret());
    }

    private static Class<?> inOutGetterReturnType(Type type)
    {
        Class<?> javaType = type.getJavaType();
        if (javaType.equals(boolean.class)) {
            return boolean.class;
        }
        if (javaType.equals(long.class)) {
            return long.class;
        }
        if (javaType.equals(double.class)) {
            return double.class;
        }
        return Object.class;
    }

    private static String inOutGetterName(Type type)
    {
        Class<?> javaType = type.getJavaType();
        if (javaType.equals(boolean.class)) {
            return "getBooleanValue";
        }
        if (javaType.equals(long.class)) {
            return "getLongValue";
        }
        if (javaType.equals(double.class)) {
            return "getDoubleValue";
        }
        return "getObjectValue";
    }

    public static <T extends AccumulatorState> AccumulatorStateFactory<T> generateStateFactory(Class<T> clazz)
    {
        return generateStateFactory(clazz, ImmutableMap.of());
    }

    @VisibleForTesting
    static <T extends AccumulatorState> AccumulatorStateFactory<T> generateStateFactory(Class<T> clazz, Map<String, Type> fieldTypes)
    {
        AccumulatorStateMetadata metadata = getMetadataAnnotation(clazz);
        if (metadata != null && metadata.stateFactoryClass() != AccumulatorStateFactory.class) {
            try {
                //noinspection unchecked
                return (AccumulatorStateFactory<T>) metadata.stateFactoryClass().getConstructor().newInstance();
            }
            catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }

        Lookup lookupInClass = getFullAccessLookupIn(clazz);

        Class<? extends T> singleStateClass = generateSingleStateClass(lookupInClass, clazz, fieldTypes);
        Class<? extends T> groupedStateClass = generateGroupedStateClass(lookupInClass, clazz, fieldTypes);

        return generateStateFactory(clazz, singleStateClass, groupedStateClass);
    }

    private static Lookup getFullAccessLookupIn(Class<?> clazz)
    {
        ClassBuilder classBuilder = ClassBuilder.createStandardClass(
                lookup().in(clazz),
                a(PUBLIC, FINAL),
                clazz.getSimpleName() + "Lookup",
                type(Object.class));

        // Private method to fetch a full access Lookup for this generated class
        // This is required because this class is in a separate classloader and thus a different unnamed module
        // and there does not seem any other way to get a full access method handle across class loaders
        classBuilder.declareMethod(a(PRIVATE, STATIC), "lookup", type(Lookup.class))
                .getBody()
                .append(invokeStatic(MethodHandles.class, "lookup", Lookup.class).ret());

        Class<?> lookupClass = classBuilder.defineClass();
        try {
            return (Lookup) privateLookupIn(lookupClass, lookup()).findStatic(lookupClass, "lookup", methodType(Lookup.class)).invoke();
        }
        catch (Throwable e) {
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    private static <T extends AccumulatorState> AccumulatorStateFactory<T> generateStateFactory(
            Class<T> clazz,
            Class<? extends T> singleStateClass,
            Class<? extends T> groupedStateClass)
    {
        ClassBuilder classBuilder = ClassBuilder.createHiddenClass(
                lookup(),
                a(PUBLIC, FINAL),
                clazz.getSimpleName() + "Factory",
                type(Object.class),
                type(AccumulatorStateFactory.class));

        // Generate constructor
        classBuilder.declareDefaultConstructor(a(PUBLIC));

        // Generate single state creation method
        classBuilder.declareMethod(a(PUBLIC), "createSingleState", type(AccumulatorState.class))
                .getBody()
                .append(classBuilder.invoke(lookupConstructor(singleStateClass), "createSingleState").ret());

        // Generate grouped state creation method
        classBuilder.declareMethod(a(PUBLIC), "createGroupedState", type(AccumulatorState.class))
                .getBody()
                .append(classBuilder.invoke(lookupConstructor(groupedStateClass), "groupedConstructor").ret());

        Class<?> factoryClass = classBuilder.defineClass(AccumulatorStateFactory.class);
        try {
            //noinspection unchecked
            return (AccumulatorStateFactory<T>) factoryClass.getConstructor().newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static <T extends AccumulatorState> MethodHandle lookupConstructor(Class<? extends T> stateClass)
    {
        try {
            return lookup().in(stateClass).findConstructor(stateClass, methodType(void.class)).asType(methodType(AccumulatorState.class));
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static <T> Class<? extends T> generateSingleStateClass(Lookup lookup, Class<T> clazz, Map<String, Type> fieldTypes)
    {
        ClassBuilder classBuilder = ClassBuilder.createHiddenClass(
                lookup,
                a(PUBLIC, FINAL),
                "Single" + clazz.getSimpleName(),
                type(Object.class),
                type(clazz));

        FieldDefinition instanceSize = generateInstanceSize(classBuilder);

        // Add getter for class size
        classBuilder.declareMethod(a(PUBLIC), "getEstimatedSize", type(long.class))
                .getBody()
                .getStaticField(instanceSize)
                .retLong();

        // Generate constructor
        MethodDefinition constructor = classBuilder.declareConstructor(a(PUBLIC));

        constructor.getBody()
                .append(constructor.getThis())
                .invokeConstructor(Object.class);

        // Generate fields
        List<StateField> fields = enumerateFields(clazz, fieldTypes);
        List<FieldDefinition> fieldDefinitions = new ArrayList<>();
        for (StateField field : fields) {
            fieldDefinitions.add(generateField(classBuilder, constructor, field));
        }

        constructor.getBody()
                .ret();

        generateCopy(classBuilder, fields, fieldDefinitions);

        return classBuilder.defineClass(clazz);
    }

    private static void generateCopy(ClassBuilder classBuilder, List<StateField> fields, List<FieldDefinition> fieldDefinitions)
    {
        MethodDefinition copy = classBuilder.declareMethod(a(PUBLIC), "copy", type(AccumulatorState.class));
        Variable thisVariable = copy.getThis();
        BytecodeBlock body = copy.getBody();

        List<BytecodeExpression> fieldCopyExpressions = new ArrayList<>();
        for (int i = 0; i < fields.size(); i++) {
            Optional<BytecodeExpression> fieldCopy = copyField(thisVariable, fieldDefinitions.get(i), fields.get(i).type());
            if (fieldCopy.isEmpty()) {
                body
                        .append(newInstance(UnsupportedOperationException.class, constantString(format("copy not supported for %s (cannot copy field of type %s)", classBuilder.getType().getClassName(), fields.get(i).type()))))
                        .throwObject();

                return;
            }

            fieldCopyExpressions.add(fieldCopy.get());
        }

        Variable instanceCopy = copy.getScope().declareVariable(classBuilder.getType(), "instanceCopy");
        body.append(instanceCopy.set(newInstance(classBuilder.getType())));

        for (int i = 0; i < fieldDefinitions.size(); i++) {
            FieldDefinition fieldDefinition = fieldDefinitions.get(i);
            Class<?> type = fields.get(i).type();

            if (type == long.class || type == double.class || type == boolean.class || type == byte.class || type == int.class) {
                body.append(instanceCopy.setField(fieldDefinition, fieldCopyExpressions.get(i)));
            }
            else {
                body.append(new IfStatement("if field value is null")
                        .condition(isNull(thisVariable.getField(fieldDefinition)))
                        .ifTrue(instanceCopy.setField(fieldDefinition, thisVariable.getField(fieldDefinition)))
                        .ifFalse(instanceCopy.setField(fieldDefinition, fieldCopyExpressions.get(i))));
            }
        }
        copy.getBody()
                .append(instanceCopy.ret());
    }

    private static Optional<BytecodeExpression> copyField(Variable thisVariable, FieldDefinition fieldDefinition, Class<?> type)
    {
        if (type == long.class || type == double.class || type == boolean.class || type == byte.class || type == int.class) {
            return Optional.of(thisVariable.getField(fieldDefinition));
        }

        if (type == Block.class) {
            return Optional.of(thisVariable.getField(fieldDefinition).invoke("copyRegion", Block.class, constantInt(0), thisVariable.getField(fieldDefinition).invoke("getPositionCount", int.class)));
        }

        return Optional.empty();
    }

    private static FieldDefinition generateInstanceSize(ClassBuilder classBuilder)
    {
        // Store instance size in static field
        FieldDefinition instanceSize = classBuilder.declareField(a(PRIVATE, STATIC, FINAL), "INSTANCE_SIZE", long.class);
        classBuilder.getClassInitializer()
                .getBody()
                .append(setStatic(instanceSize, invokeStatic(SizeOf.class, "instanceSize", int.class, constantClass(classBuilder.getType())).cast(long.class)));
        return instanceSize;
    }

    private static <T> Class<? extends T> generateGroupedStateClass(Lookup lookup, Class<T> clazz, Map<String, Type> fieldTypes)
    {
        ClassBuilder classBuilder = ClassBuilder.createHiddenClass(
                lookup,
                a(PUBLIC, FINAL),
                "Grouped" + clazz.getSimpleName(),
                type(Object.class),
                type(GroupedAccumulatorState.class),
                type(clazz));

        FieldDefinition instanceSize = generateInstanceSize(classBuilder);

        List<StateField> fields = enumerateFields(clazz, fieldTypes);

        // Create constructor
        MethodDefinition constructor = classBuilder.declareConstructor(a(PUBLIC));
        constructor.getBody()
                .append(constructor.getThis())
                .invokeConstructor(Object.class);

        // GroupId
        FieldDefinition groupIdField = classBuilder.declareField(a(PRIVATE), "groupId", long.class);
        Parameter groupIdParameter = arg("groupId", long.class);
        MethodDefinition setGroupId = classBuilder.declareMethod(a(PUBLIC), "setGroupId", type(void.class), groupIdParameter);
        setGroupId.getBody()
                .append(setGroupId.getThis().setField(groupIdField, groupIdParameter))
                .ret();

        // Create ensureCapacity
        MethodDefinition ensureCapacity = classBuilder.declareMethod(a(PUBLIC), "ensureCapacity", type(void.class), arg("size", long.class));

        // Generate getEstimatedSize
        MethodDefinition getEstimatedSize = classBuilder.declareMethod(a(PUBLIC), "getEstimatedSize", type(long.class));
        // initialize size to the size of the instance
        Variable size = getEstimatedSize.getScope().declareVariable(long.class, "size");
        getEstimatedSize.getBody().append(size.set(getStatic(instanceSize)));

        // Generate fields, constructor, and ensureCapacity
        for (StateField field : fields) {
            generateGroupedField(classBuilder, groupIdField, constructor, ensureCapacity, getEstimatedSize, field);
        }

        constructor.getBody().ret();
        ensureCapacity.getBody().ret();
        getEstimatedSize.getBody().append(size.ret());

        return classBuilder.defineClass(clazz);
    }

    private static FieldDefinition generateField(ClassBuilder classBuilder, MethodDefinition constructor, StateField stateField)
    {
        FieldDefinition field = classBuilder.declareField(a(PRIVATE), UPPER_CAMEL.to(LOWER_CAMEL, stateField.name()) + "Value", stateField.type());

        // Generate getter
        MethodDefinition getter = classBuilder.declareMethod(a(PUBLIC), stateField.getterName(), type(stateField.type()));
        getter.getBody()
                .append(getter.getThis().getField(field).cast(getter.getReturnType()).ret());

        // Generate setter
        Parameter value = arg("value", stateField.type());
        MethodDefinition setter = classBuilder.declareMethod(a(PUBLIC), stateField.setterName(), type(void.class), value);
        setter.getBody()
                .append(setter.getThis().setField(field, value))
                .ret();

        constructor.getBody()
                .append(constructor.getThis().setField(field, stateField.initialValueExpression()));

        return field;
    }

    private static void generateGroupedField(
            ClassBuilder classBuilder,
            FieldDefinition groupIdField,
            MethodDefinition constructor,
            MethodDefinition ensureCapacity,
            MethodDefinition getEstimatedSize,
            StateField stateField)
    {
        BigArrayType bigArrayType = getBigArrayType(stateField.type());
        FieldDefinition field = classBuilder.declareField(a(PRIVATE), UPPER_CAMEL.to(LOWER_CAMEL, stateField.name()) + "Values", Object.class);

        // Generate getter
        MethodDefinition getter = classBuilder.declareMethod(a(PUBLIC), stateField.getterName(), type(stateField.type()));
        getter.getBody()
                .append(classBuilder.invoke(
                        bigArrayType.getGetter(),
                        "get",
                        getter.getThis().getField(field),
                        getter.getThis().getField(groupIdField))
                        .cast(getter.getReturnType())
                        .ret());

        // Generate setter
        Parameter value = arg("value", stateField.type());
        MethodDefinition setter = classBuilder.declareMethod(a(PUBLIC), stateField.setterName(), type(void.class), value);
        setter.getBody()
                .append(classBuilder.invoke(
                        bigArrayType.getSetter(),
                        "set",
                        setter.getThis().getField(field),
                        setter.getThis().getField(groupIdField),
                        value))
                .ret();

        ensureCapacity.getBody()
                .append(classBuilder.invoke(
                        bigArrayType.getEnsureCapacity(),
                        "ensureCapacity",
                        ensureCapacity.getThis().getField(field),
                        ensureCapacity.getScope().getVariable("size")));

        // add field to size
        Variable size = getEstimatedSize.getScope().getVariable("size");
        getEstimatedSize.getBody()
                .append(size.set(add(
                    size,
                    classBuilder.invoke(
                            bigArrayType.getSizeOf(),
                            "sizeOf",
                            getEstimatedSize.getThis().getField(field)))));

        // Initialize field in constructor
        constructor.getBody()
                .append(constructor.getThis().setField(
                        field,
                        classBuilder.invoke(
                                bigArrayType.getConstructor(),
                                "init",
                                stateField.initialValueExpression())));
    }

    /**
     * Enumerates all the fields in this state interface.
     *
     * @param clazz a subclass of AccumulatorState
     * @param fieldTypes a map of field name and Type
     * @return list of state fields. Ordering is guaranteed to be stable, and have all primitive fields at the beginning.
     */
    private static List<StateField> enumerateFields(Class<?> clazz, Map<String, Type> fieldTypes)
    {
        ImmutableList.Builder<StateField> builder = ImmutableList.builder();
        for (Method method : clazz.getMethods()) {
            // ignore default methods
            if (method.isDefault() || method.getName().equals("getEstimatedSize")) {
                continue;
            }
            if (method.getName().startsWith("get")) {
                Class<?> type = method.getReturnType();
                String name = method.getName().substring(3);
                builder.add(new StateField(name, type, getInitialValue(method), method.getName(), Optional.ofNullable(fieldTypes.get(name))));
            }
            if (method.getName().startsWith("is")) {
                Class<?> type = method.getReturnType();
                checkArgument(type == boolean.class, "Only boolean is support for 'is' methods");
                String name = method.getName().substring(2);
                builder.add(new StateField(name, type, getInitialValue(method), method.getName(), Optional.of(BOOLEAN)));
            }
        }

        // We need this ordering because the serializer and deserializer are on different machines, and so the ordering of fields must be stable
        // NOTE min_max_by depends on this exact ordering, so any changes here will break it
        // TODO remove this when transition from state classes to multiple intermediates is complete
        Ordering<StateField> ordering = new Ordering<>()
        {
            @Override
            public int compare(StateField left, StateField right)
            {
                return left.name().compareTo(right.name());
            }
        };
        List<StateField> fields = ordering.sortedCopy(builder.build());
        checkInterface(clazz, fields);

        return fields;
    }

    private static Object getInitialValue(Method method)
    {
        Object value = null;

        for (Annotation annotation : method.getAnnotations()) {
            if (annotation instanceof InitialLongValue) {
                checkArgument(value == null, "%s has multiple initialValue annotations", method.getName());
                checkArgument(method.getReturnType() == long.class, "%s does not return a long, but is annotated with @InitialLongValue", method.getName());
                value = ((InitialLongValue) annotation).value();
            }
            else if (annotation instanceof InitialDoubleValue) {
                checkArgument(value == null, "%s has multiple initialValue annotations", method.getName());
                checkArgument(method.getReturnType() == double.class, "%s does not return a double, but is annotated with @InitialDoubleValue", method.getName());
                value = ((InitialDoubleValue) annotation).value();
            }
            else if (annotation instanceof InitialBooleanValue) {
                checkArgument(value == null, "%s has multiple initialValue annotations", method.getName());
                checkArgument(method.getReturnType() == boolean.class, "%s does not return a boolean, but is annotated with @InitialBooleanValue", method.getName());
                value = ((InitialBooleanValue) annotation).value();
            }
        }

        return value;
    }

    private static void checkInterface(Class<?> clazz, List<StateField> fields)
    {
        checkArgument(clazz.isInterface(), clazz.getName() + " is not an interface");
        Set<String> setters = new HashSet<>();
        Set<String> getters = new HashSet<>();
        Set<String> isGetters = new HashSet<>();

        Map<String, Class<?>> fieldTypes = new HashMap<>();
        for (StateField field : fields) {
            fieldTypes.put(field.name(), field.type());
        }

        for (Method method : clazz.getMethods()) {
            // ignore static and default methods
            if (Modifier.isStatic(method.getModifiers()) || method.isDefault()) {
                continue;
            }

            if (method.getName().equals("copy")) {
                checkArgument(method.getParameterTypes().length == 0, "copy may not have parameters");
                continue;
            }

            if (method.getName().equals("getEstimatedSize")) {
                checkArgument(method.getReturnType().equals(long.class), "getEstimatedSize must return long");
                checkArgument(method.getParameterTypes().length == 0, "getEstimatedSize may not have parameters");
                continue;
            }

            if (method.getName().startsWith("get")) {
                String name = method.getName().substring(3);
                checkArgument(fieldTypes.get(name).equals(method.getReturnType()),
                        "Expected %s to return type %s, but found %s", method.getName(), fieldTypes.get(name), method.getReturnType());
                checkArgument(method.getParameterTypes().length == 0, "Expected %s to have zero parameters", method.getName());
                getters.add(name);
            }
            else if (method.getName().startsWith("is")) {
                String name = method.getName().substring(2);
                checkArgument(fieldTypes.get(name) == boolean.class,
                        "Expected %s to have type boolean, but found %s", name, fieldTypes.get(name));
                checkArgument(method.getParameterTypes().length == 0, "Expected %s to have zero parameters", method.getName());
                checkArgument(method.getReturnType() == boolean.class, "Expected %s to return boolean", method.getName());
                isGetters.add(name);
            }
            else if (method.getName().startsWith("set")) {
                String name = method.getName().substring(3);
                checkArgument(method.getParameterTypes().length == 1, "Expected setter to have one parameter");
                checkArgument(fieldTypes.get(name).equals(method.getParameterTypes()[0]),
                        "Expected %s to accept type %s, but found %s", method.getName(), fieldTypes.get(name), method.getParameterTypes()[0]);
                checkArgument(getInitialValue(method) == null, "initial value annotation not allowed on setter");
                checkArgument(method.getReturnType().equals(void.class), "%s may not return a value", method.getName());
                setters.add(name);
            }
            else {
                throw new IllegalArgumentException("Cannot generate implementation for method: " + method.getName());
            }
        }
        checkArgument(getters.size() + isGetters.size() == setters.size() && setters.size() == fields.size(), "Wrong number of getters/setters");
    }

    private record StateField(String name, Class<?> type, Object initialValue, String getterName, Optional<Type> sqlTypeOptional)
    {
        private StateField(String name, Class<?> type, Object initialValue, String getterName, Optional<Type> sqlTypeOptional)
        {
            this.name = requireNonNull(name, "name is null");
            checkArgument(!name.isEmpty(), "name is empty");
            this.type = requireNonNull(type, "type is null");
            this.getterName = requireNonNull(getterName, "getterName is null");
            this.initialValue = initialValue;
            requireNonNull(sqlTypeOptional, "sqlType is null");
            if (sqlTypeOptional.isPresent()) {
                checkArgument(
                        type.isAssignableFrom(sqlTypeOptional.get().getJavaType()) ||
                                ((type == byte.class) && TINYINT.equals(sqlTypeOptional.get())) ||
                                ((type == int.class) && INTEGER.equals(sqlTypeOptional.get())),
                        "Stack type (%s) and provided sql type (%s) are incompatible", type.getName(), sqlTypeOptional.get().getDisplayName());
            }
            else {
                sqlTypeOptional = sqlTypeFromStackType(type);
            }
            this.sqlTypeOptional = sqlTypeOptional;
        }

        private static Optional<Type> sqlTypeFromStackType(Class<?> stackType)
        {
            if (stackType == long.class) {
                return Optional.of(BIGINT);
            }
            if (stackType == double.class) {
                return Optional.of(DOUBLE);
            }
            if (stackType == boolean.class) {
                return Optional.of(BOOLEAN);
            }
            if (stackType == byte.class) {
                return Optional.of(TINYINT);
            }
            if (stackType == int.class) {
                return Optional.of(INTEGER);
            }
            if (stackType == Slice.class) {
                return Optional.of(VARBINARY);
            }
            return Optional.empty();
        }

        String setterName()
        {
            return "set" + name();
        }

        Type sqlType()
        {
            return sqlTypeOptional.orElseThrow(() -> new IllegalArgumentException("Unsupported type: " + type));
        }

        boolean isPrimitiveType()
        {
            Class<?> type = type();
            return (type == long.class || type == double.class || type == boolean.class || type == byte.class || type == int.class);
        }

        public BytecodeExpression initialValueExpression()
        {
            if (initialValue == null) {
                return defaultValue(type);
            }
            if (initialValue instanceof Number) {
                return constantNumber((Number) initialValue);
            }
            if (initialValue instanceof Boolean) {
                return constantBoolean((boolean) initialValue);
            }
            throw new IllegalArgumentException("Unsupported initial value type: " + initialValue.getClass());
        }
    }

    private static BigArrayType getBigArrayType(Class<?> type)
    {
        try {
            if (type.equals(long.class)) {
                return new BigArrayType(LongBigArray.class, long.class);
            }
            if (type.equals(byte.class)) {
                return new BigArrayType(ByteBigArray.class, byte.class);
            }
            if (type.equals(double.class)) {
                return new BigArrayType(DoubleBigArray.class, double.class);
            }
            if (type.equals(boolean.class)) {
                return new BigArrayType(BooleanBigArray.class, boolean.class);
            }
            if (type.equals(int.class)) {
                return new BigArrayType(IntBigArray.class, int.class);
            }
            if (type.equals(Slice.class)) {
                return new BigArrayType(SliceBigArray.class, Slice.class);
            }
            if (type.equals(Block.class)) {
                return new BigArrayType(BlockBigArray.class, Block.class);
            }
            return new BigArrayType(ObjectBigArray.class, Object.class);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static final class BigArrayType
    {
        private final Class<?> bigArrayType;
        private final MethodHandle constructor;
        private final MethodHandle ensureCapacity;
        private final MethodHandle sizeOf;
        private final MethodHandle getter;
        private final MethodHandle setter;

        public BigArrayType(Class<?> bigArrayType, Class<?> elementType)
                throws ReflectiveOperationException
        {
            this.bigArrayType = bigArrayType;
            constructor = lookup().findConstructor(bigArrayType, methodType(void.class, elementType))
                    .asType(methodType(Object.class, elementType));
            ensureCapacity = lookup().findVirtual(bigArrayType, "ensureCapacity", methodType(void.class, long.class))
                    .asType(methodType(void.class, Object.class, long.class));
            sizeOf = lookup().findVirtual(bigArrayType, "sizeOf", methodType(long.class))
                    .asType(methodType(long.class, Object.class));
            getter = lookup().findVirtual(bigArrayType, "get", methodType(elementType, long.class))
                    .asType(methodType(elementType, Object.class, long.class));
            setter = lookup().findVirtual(bigArrayType, "set", methodType(void.class, long.class, elementType))
                    .asType(methodType(void.class, Object.class, long.class, elementType));
        }

        public Class<?> getBigArrayType()
        {
            return bigArrayType;
        }

        public MethodHandle getConstructor()
        {
            return constructor;
        }

        public MethodHandle getEnsureCapacity()
        {
            return ensureCapacity;
        }

        public MethodHandle getSizeOf()
        {
            return sizeOf;
        }

        public MethodHandle getGetter()
        {
            return getter;
        }

        public MethodHandle getSetter()
        {
            return setter;
        }
    }
}
