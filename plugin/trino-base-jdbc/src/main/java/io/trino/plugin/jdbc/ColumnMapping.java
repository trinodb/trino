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
package io.trino.plugin.jdbc;

import io.trino.spi.type.Type;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;
import static java.util.Objects.requireNonNull;

public final class ColumnMapping
{
    public static ColumnMapping booleanMapping(Type trinoType, BooleanReadFunction readFunction, BooleanWriteFunction writeFunction)
    {
        return booleanMapping(trinoType, readFunction, writeFunction, FULL_PUSHDOWN);
    }

    public static ColumnMapping booleanMapping(
            Type trinoType,
            BooleanReadFunction readFunction,
            BooleanWriteFunction writeFunction,
            PredicatePushdownController predicatePushdownController)
    {
        return new ColumnMapping(trinoType, readFunction, writeFunction, predicatePushdownController);
    }

    public static ColumnMapping longMapping(Type trinoType, LongReadFunction readFunction, LongWriteFunction writeFunction)
    {
        return longMapping(trinoType, readFunction, writeFunction, FULL_PUSHDOWN);
    }

    public static ColumnMapping longMapping(
            Type trinoType,
            LongReadFunction readFunction,
            LongWriteFunction writeFunction,
            PredicatePushdownController predicatePushdownController)
    {
        return new ColumnMapping(trinoType, readFunction, writeFunction, predicatePushdownController);
    }

    public static ColumnMapping doubleMapping(Type trinoType, DoubleReadFunction readFunction, DoubleWriteFunction writeFunction)
    {
        return doubleMapping(trinoType, readFunction, writeFunction, FULL_PUSHDOWN);
    }

    public static ColumnMapping doubleMapping(
            Type trinoType,
            DoubleReadFunction readFunction,
            DoubleWriteFunction writeFunction,
            PredicatePushdownController predicatePushdownController)
    {
        return new ColumnMapping(trinoType, readFunction, writeFunction, predicatePushdownController);
    }

    public static ColumnMapping sliceMapping(Type trinoType, SliceReadFunction readFunction, SliceWriteFunction writeFunction)
    {
        return sliceMapping(trinoType, readFunction, writeFunction, FULL_PUSHDOWN);
    }

    public static ColumnMapping sliceMapping(
            Type trinoType,
            SliceReadFunction readFunction,
            SliceWriteFunction writeFunction,
            PredicatePushdownController predicatePushdownController)
    {
        return new ColumnMapping(trinoType, readFunction, writeFunction, predicatePushdownController);
    }

    public static ColumnMapping objectMapping(Type trinoType, ObjectReadFunction readFunction, ObjectWriteFunction writeFunction)
    {
        return objectMapping(trinoType, readFunction, writeFunction, FULL_PUSHDOWN);
    }

    public static ColumnMapping objectMapping(
            Type trinoType,
            ObjectReadFunction readFunction,
            ObjectWriteFunction writeFunction,
            PredicatePushdownController predicatePushdownController)
    {
        return new ColumnMapping(trinoType, readFunction, writeFunction, predicatePushdownController);
    }

    private final Type type;
    private final ReadFunction readFunction;
    private final WriteFunction writeFunction;
    private final PredicatePushdownController predicatePushdownController;

    /**
     * @deprecated Prefer factory methods instead over calling constructor directly.
     */
    @Deprecated
    public ColumnMapping(Type type, ReadFunction readFunction, WriteFunction writeFunction, PredicatePushdownController predicatePushdownController)
    {
        this.type = requireNonNull(type, "type is null");
        this.readFunction = requireNonNull(readFunction, "readFunction is null");
        this.writeFunction = requireNonNull(writeFunction, "writeFunction is null");
        checkArgument(
                type.getJavaType() == readFunction.getJavaType(),
                "Trino type %s is not compatible with read function %s returning %s",
                type,
                readFunction,
                readFunction.getJavaType());
        checkArgument(
                type.getJavaType() == writeFunction.getJavaType(),
                "Trino type %s is not compatible with write function %s accepting %s",
                type,
                writeFunction,
                writeFunction.getJavaType());
        this.predicatePushdownController = requireNonNull(predicatePushdownController, "predicatePushdownController is null");
    }

    public Type getType()
    {
        return type;
    }

    public ReadFunction getReadFunction()
    {
        return readFunction;
    }

    public WriteFunction getWriteFunction()
    {
        return writeFunction;
    }

    public PredicatePushdownController getPredicatePushdownController()
    {
        return predicatePushdownController;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", type)
                .toString();
    }
}
