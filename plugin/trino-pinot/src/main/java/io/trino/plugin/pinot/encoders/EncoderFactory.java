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
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.pinot.spi.data.FieldSpec;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.pinot.PinotTypeUtils.isSupportedType;
import static java.util.Objects.requireNonNull;

public class EncoderFactory
{
    private EncoderFactory() {}

    public static Encoder createEncoder(FieldSpec fieldSpec, Type trinoType)
    {
        requireNonNull(trinoType, "trinoType is null");
        requireNonNull(fieldSpec, "fieldSpec is null");
        checkState(isSupportedType(trinoType), "Unsupported type %s", trinoType);
        String fieldName = fieldSpec.getName();
        if (trinoType instanceof ArrayType) {
            checkState(!fieldSpec.isSingleValueField(), "Pinot and trino types are incompatible: %s, %s", trinoType, fieldSpec);
            ArrayType arrayType = (ArrayType) trinoType;
            return new ArrayEncoder(fieldSpec, arrayType.getElementType());
        }
        checkState(fieldSpec.isSingleValueField(), "Pinot and trino types are incompatible: %s, %s", trinoType, fieldSpec);
        if (trinoType instanceof BigintType) {
            return new BigintEncoder();
        }
        else if (trinoType instanceof IntegerType) {
            return new IntegerEncoder();
        }
        else if (trinoType instanceof SmallintType) {
            return new SmallintEncoder();
        }
        else if (trinoType instanceof TinyintType) {
            return new TinyintEncoder();
        }
        else if (trinoType instanceof DoubleType) {
            return new DoubleEncoder();
        }
        else if (trinoType instanceof RealType) {
            return new RealEncoder();
        }
        else if (trinoType instanceof VarcharType) {
            return new VarcharEncoder();
        }
        throw new UnsupportedOperationException();
    }
}
