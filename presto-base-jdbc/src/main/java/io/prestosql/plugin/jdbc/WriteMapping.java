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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class WriteMapping
{
    public static final WriteNullFunction DEFAULT_WRITE_NULL_FUNCTION = (statement, index) -> statement.setObject(index, null);

    public static WriteMapping booleanMapping(String dataType, BooleanWriteFunction writeFunction)
    {
        return booleanMapping(dataType, writeFunction, DEFAULT_WRITE_NULL_FUNCTION);
    }

    public static WriteMapping booleanMapping(String dataType, BooleanWriteFunction writeFunction, WriteNullFunction writeNullFunction)
    {
        return new WriteMapping(dataType, writeFunction, writeNullFunction);
    }

    public static WriteMapping longMapping(String dataType, LongWriteFunction writeFunction)
    {
        return longMapping(dataType, writeFunction, DEFAULT_WRITE_NULL_FUNCTION);
    }

    public static WriteMapping longMapping(String dataType, LongWriteFunction writeFunction, WriteNullFunction writeNullFunction)
    {
        return new WriteMapping(dataType, writeFunction, writeNullFunction);
    }

    public static WriteMapping doubleMapping(String dataType, DoubleWriteFunction writeFunction)
    {
        return doubleMapping(dataType, writeFunction, DEFAULT_WRITE_NULL_FUNCTION);
    }

    public static WriteMapping doubleMapping(String dataType, DoubleWriteFunction writeFunction, WriteNullFunction writeNullFunction)
    {
        return new WriteMapping(dataType, writeFunction, writeNullFunction);
    }

    public static WriteMapping sliceMapping(String dataType, SliceWriteFunction writeFunction)
    {
        return sliceMapping(dataType, writeFunction, DEFAULT_WRITE_NULL_FUNCTION);
    }

    public static WriteMapping sliceMapping(String dataType, SliceWriteFunction writeFunction, WriteNullFunction writeNullFunction)
    {
        return new WriteMapping(dataType, writeFunction, writeNullFunction);
    }

    public static WriteMapping blockMapping(String dataType, BlockWriteFunction writeFunction)
    {
        return blockMapping(dataType, writeFunction, DEFAULT_WRITE_NULL_FUNCTION);
    }

    public static WriteMapping blockMapping(String dataType, BlockWriteFunction writeFunction, WriteNullFunction defaultWriteNullFunction)
    {
        return new WriteMapping(dataType, writeFunction, defaultWriteNullFunction);
    }

    private final String dataType;
    private final WriteFunction writeFunction;
    private final WriteNullFunction writeNullFunction;

    private WriteMapping(String dataType, WriteFunction writeFunction, WriteNullFunction writeNullFunction)
    {
        this.dataType = requireNonNull(dataType, "dataType is null");
        this.writeFunction = requireNonNull(writeFunction, "writeFunction is null");
        this.writeNullFunction = requireNonNull(writeNullFunction, "writeNullFunction is null");
    }

    /**
     * Data type that should be used in the remote database when defining a column.
     */
    public String getDataType()
    {
        return dataType;
    }

    public WriteFunction getWriteFunction()
    {
        return writeFunction;
    }

    public WriteNullFunction getWriteNullFunction()
    {
        return writeNullFunction;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("dataType", dataType)
                .toString();
    }
}
