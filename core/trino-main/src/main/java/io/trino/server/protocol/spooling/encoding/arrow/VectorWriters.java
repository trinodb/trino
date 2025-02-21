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
package io.trino.server.protocol.spooling.encoding.arrow;

import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;

public final class VectorWriters
{
    private VectorWriters() {}

    public static ArrowWriter writerForVector(ValueVector valueVector, Type type)
    {
        return switch (valueVector) {
            case BitVector vector -> new BooleanWriter(vector);
            case TinyIntVector vector -> new TinyIntWriter(vector);
            case SmallIntVector vector -> new SmallIntWriter(vector);
            case IntVector vector -> new IntegerWriter(vector);
            case BigIntVector vector -> new BigintWriter(vector);
            case Float4Vector vector -> new RealWriter(vector);
            case Float8Vector vector -> new DoubleWriter(vector);
            case VarCharVector vector -> type instanceof CharType charType ? new CharWriter(vector, charType) : new VarcharWriter(vector);
            case VarBinaryVector vector -> new VarbinaryWriter(vector);
            case DateDayVector vector -> new DateWriter(vector);
            case DecimalVector vector -> new DecimalWriter(vector, (DecimalType) type);
            case IntervalDayVector vector -> new IntervalDayWriter(vector);
            case FixedSizeBinaryVector vector -> new UuidWriter(vector);
            case TimeSecVector vector -> new TimeSecWriter(vector);
            case TimeMilliVector vector -> new TimeMilliWriter(vector);
            case TimeMicroVector vector -> new TimeMicroWriter(vector);
            case TimeNanoVector vector -> new TimeNanoWriter(vector);
            case TimeStampSecVector vector -> new TimestampSecWriter(vector);
            case TimeStampMilliVector vector -> new TimestampMilliWriter(vector);
            case TimeStampMicroVector vector -> new TimestampMicroWriter(vector);
            case TimeStampNanoVector vector -> new TimestampNanoWriter(vector);
            case MapVector vector -> new MapWriter(vector, (MapType) type);
            case ListVector vector -> new ArrayWriter(vector, (ArrayType) type);
            case StructVector vector -> new RowWriter(vector, (RowType) type);
            case NullVector vector -> new NullWriter(vector);
            default -> throw new UnsupportedOperationException("Unsupported vector type: " + valueVector.getClass().getName());
        };
    }
}
