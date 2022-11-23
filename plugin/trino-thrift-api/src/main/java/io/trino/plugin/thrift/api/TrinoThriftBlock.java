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
package io.trino.plugin.thrift.api;

import io.airlift.drift.annotations.ThriftConstructor;
import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftStruct;
import io.airlift.slice.Slice;
import io.trino.plugin.thrift.api.datatypes.TrinoThriftBigint;
import io.trino.plugin.thrift.api.datatypes.TrinoThriftBigintArray;
import io.trino.plugin.thrift.api.datatypes.TrinoThriftBoolean;
import io.trino.plugin.thrift.api.datatypes.TrinoThriftColumnData;
import io.trino.plugin.thrift.api.datatypes.TrinoThriftDate;
import io.trino.plugin.thrift.api.datatypes.TrinoThriftDouble;
import io.trino.plugin.thrift.api.datatypes.TrinoThriftHyperLogLog;
import io.trino.plugin.thrift.api.datatypes.TrinoThriftInteger;
import io.trino.plugin.thrift.api.datatypes.TrinoThriftJson;
import io.trino.plugin.thrift.api.datatypes.TrinoThriftTimestamp;
import io.trino.plugin.thrift.api.datatypes.TrinoThriftVarchar;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import javax.annotation.Nullable;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.drift.annotations.ThriftField.Requiredness.OPTIONAL;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.StandardTypes.HYPER_LOG_LOG;
import static io.trino.spi.type.StandardTypes.JSON;

@ThriftStruct
public final class TrinoThriftBlock
{
    // number
    private final TrinoThriftInteger integerData;
    private final TrinoThriftBigint bigintData;
    private final TrinoThriftDouble doubleData;

    // variable width
    private final TrinoThriftVarchar varcharData;

    // boolean
    private final TrinoThriftBoolean booleanData;

    // temporal
    private final TrinoThriftDate dateData;
    private final TrinoThriftTimestamp timestampData;

    // special
    private final TrinoThriftJson jsonData;
    private final TrinoThriftHyperLogLog hyperLogLogData;

    // array
    private final TrinoThriftBigintArray bigintArrayData;

    // non-thrift field which points to non-null data item
    private final TrinoThriftColumnData dataReference;

    @ThriftConstructor
    public TrinoThriftBlock(
            @Nullable TrinoThriftInteger integerData,
            @Nullable TrinoThriftBigint bigintData,
            @Nullable TrinoThriftDouble doubleData,
            @Nullable TrinoThriftVarchar varcharData,
            @Nullable TrinoThriftBoolean booleanData,
            @Nullable TrinoThriftDate dateData,
            @Nullable TrinoThriftTimestamp timestampData,
            @Nullable TrinoThriftJson jsonData,
            @Nullable TrinoThriftHyperLogLog hyperLogLogData,
            @Nullable TrinoThriftBigintArray bigintArrayData)
    {
        this.integerData = integerData;
        this.bigintData = bigintData;
        this.doubleData = doubleData;
        this.varcharData = varcharData;
        this.booleanData = booleanData;
        this.dateData = dateData;
        this.timestampData = timestampData;
        this.jsonData = jsonData;
        this.hyperLogLogData = hyperLogLogData;
        this.bigintArrayData = bigintArrayData;
        this.dataReference = theOnlyNonNull(integerData, bigintData, doubleData, varcharData, booleanData, dateData, timestampData, jsonData, hyperLogLogData, bigintArrayData);
    }

    @Nullable
    @ThriftField(value = 1, requiredness = OPTIONAL)
    public TrinoThriftInteger getIntegerData()
    {
        return integerData;
    }

    @Nullable
    @ThriftField(value = 2, requiredness = OPTIONAL)
    public TrinoThriftBigint getBigintData()
    {
        return bigintData;
    }

    @Nullable
    @ThriftField(value = 3, requiredness = OPTIONAL)
    public TrinoThriftDouble getDoubleData()
    {
        return doubleData;
    }

    @Nullable
    @ThriftField(value = 4, requiredness = OPTIONAL)
    public TrinoThriftVarchar getVarcharData()
    {
        return varcharData;
    }

    @Nullable
    @ThriftField(value = 5, requiredness = OPTIONAL)
    public TrinoThriftBoolean getBooleanData()
    {
        return booleanData;
    }

    @Nullable
    @ThriftField(value = 6, requiredness = OPTIONAL)
    public TrinoThriftDate getDateData()
    {
        return dateData;
    }

    @Nullable
    @ThriftField(value = 7, requiredness = OPTIONAL)
    public TrinoThriftTimestamp getTimestampData()
    {
        return timestampData;
    }

    @Nullable
    @ThriftField(value = 8, requiredness = OPTIONAL)
    public TrinoThriftJson getJsonData()
    {
        return jsonData;
    }

    @Nullable
    @ThriftField(value = 9, requiredness = OPTIONAL)
    public TrinoThriftHyperLogLog getHyperLogLogData()
    {
        return hyperLogLogData;
    }

    @Nullable
    @ThriftField(value = 10, requiredness = OPTIONAL)
    public TrinoThriftBigintArray getBigintArrayData()
    {
        return bigintArrayData;
    }

    public Block toBlock(Type desiredType)
    {
        return dataReference.toBlock(desiredType);
    }

    public int numberOfRecords()
    {
        return dataReference.numberOfRecords();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TrinoThriftBlock other = (TrinoThriftBlock) obj;
        // remaining fields are guaranteed to be null by the constructor
        return Objects.equals(this.dataReference, other.dataReference);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(integerData, bigintData, doubleData, varcharData, booleanData, dateData, timestampData, jsonData, hyperLogLogData, bigintArrayData);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("data", dataReference)
                .toString();
    }

    public static TrinoThriftBlock integerData(TrinoThriftInteger integerData)
    {
        return new TrinoThriftBlock(integerData, null, null, null, null, null, null, null, null, null);
    }

    public static TrinoThriftBlock bigintData(TrinoThriftBigint bigintData)
    {
        return new TrinoThriftBlock(null, bigintData, null, null, null, null, null, null, null, null);
    }

    public static TrinoThriftBlock doubleData(TrinoThriftDouble doubleData)
    {
        return new TrinoThriftBlock(null, null, doubleData, null, null, null, null, null, null, null);
    }

    public static TrinoThriftBlock varcharData(TrinoThriftVarchar varcharData)
    {
        return new TrinoThriftBlock(null, null, null, varcharData, null, null, null, null, null, null);
    }

    public static TrinoThriftBlock booleanData(TrinoThriftBoolean booleanData)
    {
        return new TrinoThriftBlock(null, null, null, null, booleanData, null, null, null, null, null);
    }

    public static TrinoThriftBlock dateData(TrinoThriftDate dateData)
    {
        return new TrinoThriftBlock(null, null, null, null, null, dateData, null, null, null, null);
    }

    public static TrinoThriftBlock timestampData(TrinoThriftTimestamp timestampData)
    {
        return new TrinoThriftBlock(null, null, null, null, null, null, timestampData, null, null, null);
    }

    public static TrinoThriftBlock jsonData(TrinoThriftJson jsonData)
    {
        return new TrinoThriftBlock(null, null, null, null, null, null, null, jsonData, null, null);
    }

    public static TrinoThriftBlock hyperLogLogData(TrinoThriftHyperLogLog hyperLogLogData)
    {
        return new TrinoThriftBlock(null, null, null, null, null, null, null, null, hyperLogLogData, null);
    }

    public static TrinoThriftBlock bigintArrayData(TrinoThriftBigintArray bigintArrayData)
    {
        return new TrinoThriftBlock(null, null, null, null, null, null, null, null, null, bigintArrayData);
    }

    public static TrinoThriftBlock fromNativeValue(Object trinoNativeValue, Type type)
    {
        return fromBlock(nativeValueToBlock(type, trinoNativeValue), type);
    }

    public static TrinoThriftBlock fromBlock(Block block, Type type)
    {
        if (type instanceof IntegerType) {
            return TrinoThriftInteger.fromBlock(block);
        }
        if (type instanceof BigintType) {
            return TrinoThriftBigint.fromBlock(block);
        }
        if (type instanceof DoubleType) {
            return TrinoThriftDouble.fromBlock(block);
        }
        if (type instanceof VarcharType) {
            return TrinoThriftVarchar.fromBlock(block, type);
        }
        if (type instanceof BooleanType) {
            return TrinoThriftBoolean.fromBlock(block);
        }
        if (type instanceof DateType) {
            return TrinoThriftDate.fromBlock(block);
        }
        if (type instanceof TimestampType) {
            return TrinoThriftTimestamp.fromBlock(block);
        }
        if (type instanceof ArrayType) {
            Type elementType = getOnlyElement(type.getTypeParameters());
            if (BigintType.BIGINT.equals(elementType)) {
                return TrinoThriftBigintArray.fromBlock(block);
            }
            throw new IllegalArgumentException("Unsupported array block type: " + type);
        }
        if (type.getBaseName().equals(JSON)) {
            return TrinoThriftJson.fromBlock(block, type);
        }
        if (type.getBaseName().equals(HYPER_LOG_LOG)) {
            return TrinoThriftHyperLogLog.fromBlock(block);
        }

        throw new IllegalArgumentException("Unsupported block type: " + type);
    }

    public static TrinoThriftBlock fromRecordSetColumn(RecordSet recordSet, int columnIndex, int totalRecords)
    {
        Type type = recordSet.getColumnTypes().get(columnIndex);
        // use more efficient implementations for numeric types which are likely to be used in index join
        if (type instanceof IntegerType) {
            return TrinoThriftInteger.fromRecordSetColumn(recordSet, columnIndex, totalRecords);
        }
        if (type instanceof BigintType) {
            return TrinoThriftBigint.fromRecordSetColumn(recordSet, columnIndex, totalRecords);
        }
        if (type instanceof DateType) {
            return TrinoThriftDate.fromRecordSetColumn(recordSet, columnIndex, totalRecords);
        }
        if (type instanceof TimestampType) {
            return TrinoThriftTimestamp.fromRecordSetColumn(recordSet, columnIndex, totalRecords);
        }
        // less efficient implementation which converts to a block first
        return fromBlock(convertColumnToBlock(recordSet, columnIndex, totalRecords), type);
    }

    private static Block convertColumnToBlock(RecordSet recordSet, int columnIndex, int positions)
    {
        Type type = recordSet.getColumnTypes().get(columnIndex);
        BlockBuilder output = type.createBlockBuilder(null, positions);
        Class<?> javaType = type.getJavaType();
        RecordCursor cursor = recordSet.cursor();
        for (int position = 0; position < positions; position++) {
            checkState(cursor.advanceNextPosition(), "cursor has less values than expected");
            if (cursor.isNull(columnIndex)) {
                output.appendNull();
            }
            else {
                if (javaType == boolean.class) {
                    type.writeBoolean(output, cursor.getBoolean(columnIndex));
                }
                else if (javaType == long.class) {
                    type.writeLong(output, cursor.getLong(columnIndex));
                }
                else if (javaType == double.class) {
                    type.writeDouble(output, cursor.getDouble(columnIndex));
                }
                else if (javaType == Slice.class) {
                    Slice slice = cursor.getSlice(columnIndex);
                    type.writeSlice(output, slice, 0, slice.length());
                }
                else {
                    type.writeObject(output, cursor.getObject(columnIndex));
                }
            }
        }
        checkState(!cursor.advanceNextPosition(), "cursor has more values than expected");
        return output.build();
    }

    private static TrinoThriftColumnData theOnlyNonNull(TrinoThriftColumnData... columnsData)
    {
        TrinoThriftColumnData result = null;
        for (TrinoThriftColumnData data : columnsData) {
            if (data != null) {
                checkArgument(result == null, "more than one type is present");
                result = data;
            }
        }
        checkArgument(result != null, "no types are present");
        return result;
    }
}
