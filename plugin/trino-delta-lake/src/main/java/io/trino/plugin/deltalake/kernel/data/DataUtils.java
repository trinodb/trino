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
package io.trino.plugin.deltalake.kernel.data;

import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import io.trino.plugin.deltalake.kernel.KernelSchemaUtils;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ByteArrayBlockBuilder;
import io.trino.spi.block.IntArrayBlockBuilder;
import io.trino.spi.block.LongArrayBlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;

public class DataUtils
{
    private DataUtils()
    {
    }

    public static BlockBuilder createBlockBuilder(TypeManager typeManager, DataType dataType, int expectedEntries)
    {
        if (dataType instanceof BooleanType || dataType instanceof ByteType) {
            return new ByteArrayBlockBuilder(null, expectedEntries);
        }
        else if (
                dataType instanceof ShortType
                        || dataType instanceof IntegerType
                        || dataType instanceof FloatType
                        || dataType instanceof DateType) {
            return new IntArrayBlockBuilder(null, expectedEntries);
        }
        else if (
                dataType instanceof LongType
                        || dataType instanceof DoubleType
                        || dataType instanceof TimestampType
                        || dataType instanceof TimestampNTZType) {
            return new LongArrayBlockBuilder(null, expectedEntries);
        }
        else if (dataType instanceof StringType || dataType instanceof BinaryType) {
            return new VariableWidthBlockBuilder(null, expectedEntries, expectedEntries * 10);
        }
        else if (dataType instanceof StructType) {
            Type trinoType = KernelSchemaUtils.toTrinoType(new SchemaTableName("test", "test"), typeManager, dataType);
            return new RowBlockBuilder(trinoType.getTypeParameters(), null, expectedEntries);
        }
        else if (dataType instanceof ArrayType) {
            Type trinoType = KernelSchemaUtils.toTrinoType(new SchemaTableName("test", "test"), typeManager, dataType);
            return new ArrayBlockBuilder(((io.trino.spi.type.ArrayType) trinoType).getElementType(), null, expectedEntries, expectedEntries * 10);
        }
        else if (dataType instanceof MapType) {
            Type trinoType = KernelSchemaUtils.toTrinoType(new SchemaTableName("test", "test"), typeManager, dataType);
            io.trino.spi.type.MapType mapTrinoType = (io.trino.spi.type.MapType) trinoType;
            return new MapBlockBuilder(mapTrinoType, null, expectedEntries);
        }
        throw new UnsupportedOperationException("Unsupported DataType " + dataType);
    }
}
