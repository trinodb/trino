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
package io.trino.plugin.pinot.encoders;

import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.pinot.spi.data.FieldSpec;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class EncoderFactory
{
    private EncoderFactory() {}

    public static Encoder createEncoder(FieldSpec fieldSpec, Type prestoType)
    {
        requireNonNull(prestoType, "prestoType is null");
        requireNonNull(fieldSpec, "fieldSpec is null");
        //checkState(isSupportedType(prestoType), "Unsupported type %s", prestoType);
        if (prestoType instanceof ArrayType) {
            checkState(!fieldSpec.isSingleValueField(), "Pinot and presto types are incompatible: %s, %s", prestoType, fieldSpec);
            ArrayType arrayType = (ArrayType) prestoType;
            return new ArrayEncoder(fieldSpec, arrayType.getElementType());
        }
        if (prestoType instanceof BigintType) {
            return new BigintEncoder();
        }
        else if (prestoType instanceof IntegerType) {
            return new IntegerEncoder();
        }
        else if (prestoType instanceof SmallintType) {
            return new SmallintEncoder();
        }
        else if (prestoType instanceof TinyintType) {
            return new TinyintEncoder();
        }
        else if (prestoType instanceof DoubleType) {
            return new DoubleEncoder();
        }
        else if (prestoType instanceof RealType) {
            return new RealEncoder();
        }
        else if (prestoType instanceof VarcharType) {
            return new VarcharEncoder();
        }
        else if (prestoType instanceof BooleanType) {
            return new BooleanEncoder();
        }
        else if (prestoType instanceof VarbinaryType) {
            return new VarbinaryEncoder();
        }
        else if (prestoType.getTypeSignature().getBase() == StandardTypes.JSON) {
            return new JsonEncoder();
        }
        else if (prestoType instanceof TimestampType) {
            return new TimestampEncoder((TimestampType) prestoType);
        }
        throw new UnsupportedOperationException();
    }
}
