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
package io.prestosql.plugin.jdbc;

import io.prestosql.plugin.jdbc.PredicatePushdownController.DomainPushdownResult;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.type.Type;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class ColumnMapping
{
    public static final PredicatePushdownController FULL_PUSHDOWN = domain -> new DomainPushdownResult(domain, Domain.all(domain.getType()));
    public static final PredicatePushdownController PUSHDOWN_AND_KEEP = domain -> new DomainPushdownResult(domain, domain);
    public static final PredicatePushdownController DISABLE_PUSHDOWN = domain -> new DomainPushdownResult(Domain.all(domain.getType()), domain);

    public static ColumnMapping booleanMapping(Type prestoType, BooleanReadFunction readFunction, BooleanWriteFunction writeFunction)
    {
        return booleanMapping(prestoType, readFunction, writeFunction, FULL_PUSHDOWN);
    }

    public static ColumnMapping booleanMapping(
            Type prestoType,
            BooleanReadFunction readFunction,
            BooleanWriteFunction writeFunction,
            PredicatePushdownController predicatePushdownController)
    {
        return new ColumnMapping(prestoType, readFunction, writeFunction, predicatePushdownController);
    }

    public static ColumnMapping longMapping(Type prestoType, LongReadFunction readFunction, LongWriteFunction writeFunction)
    {
        return longMapping(prestoType, readFunction, writeFunction, FULL_PUSHDOWN);
    }

    public static ColumnMapping longMapping(
            Type prestoType,
            LongReadFunction readFunction,
            LongWriteFunction writeFunction,
            PredicatePushdownController predicatePushdownController)
    {
        return new ColumnMapping(prestoType, readFunction, writeFunction, predicatePushdownController);
    }

    public static ColumnMapping doubleMapping(Type prestoType, DoubleReadFunction readFunction, DoubleWriteFunction writeFunction)
    {
        return doubleMapping(prestoType, readFunction, writeFunction, FULL_PUSHDOWN);
    }

    public static ColumnMapping doubleMapping(
            Type prestoType,
            DoubleReadFunction readFunction,
            DoubleWriteFunction writeFunction,
            PredicatePushdownController predicatePushdownController)
    {
        return new ColumnMapping(prestoType, readFunction, writeFunction, predicatePushdownController);
    }

    public static ColumnMapping sliceMapping(Type prestoType, SliceReadFunction readFunction, SliceWriteFunction writeFunction)
    {
        return sliceMapping(prestoType, readFunction, writeFunction, FULL_PUSHDOWN);
    }

    public static ColumnMapping sliceMapping(
            Type prestoType,
            SliceReadFunction readFunction,
            SliceWriteFunction writeFunction,
            PredicatePushdownController predicatePushdownController)
    {
        return new ColumnMapping(prestoType, readFunction, writeFunction, predicatePushdownController);
    }

    public static <T> ColumnMapping objectMapping(Type prestoType, ObjectReadFunction readFunction, ObjectWriteFunction writeFunction)
    {
        return objectMapping(prestoType, readFunction, writeFunction, FULL_PUSHDOWN);
    }

    public static <T> ColumnMapping objectMapping(
            Type prestoType,
            ObjectReadFunction readFunction,
            ObjectWriteFunction writeFunction,
            PredicatePushdownController predicatePushdownController)
    {
        return new ColumnMapping(prestoType, readFunction, writeFunction, predicatePushdownController);
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
                "Presto type %s is not compatible with read function %s returning %s",
                type,
                readFunction,
                readFunction.getJavaType());
        checkArgument(
                type.getJavaType() == writeFunction.getJavaType(),
                "Presto type %s is not compatible with write function %s accepting %s",
                type,
                writeFunction,
                writeFunction.getJavaType());
        this.predicatePushdownController = requireNonNull(predicatePushdownController, "pushdownController is null");
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
