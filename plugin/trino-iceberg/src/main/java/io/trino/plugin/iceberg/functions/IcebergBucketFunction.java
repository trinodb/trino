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
package io.trino.plugin.iceberg.functions;

import io.airlift.slice.Slice;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.StandardTypes;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;

import java.math.BigDecimal;
import java.math.BigInteger;

import static io.trino.spi.type.Decimals.MAX_SHORT_PRECISION;
import static io.trino.spi.type.StandardTypes.DATE;
import static io.trino.spi.type.StandardTypes.INTEGER;
import static io.trino.spi.type.StandardTypes.VARCHAR;

public final class IcebergBucketFunction
{
    private IcebergBucketFunction() {}

    @ScalarFunction(schema = "system", value = "bucket")
    @Description("The Iceberg bucket transform")
    @SqlType(INTEGER)
    @SqlNullable
    public static Long bucket(@SqlNullable @SqlType(VARCHAR) Slice partitionValue, @SqlType(INTEGER) long numberOfBuckets)
    {
        if (partitionValue == null) {
            return null;
        }
        return (long) Transforms.bucket((int) numberOfBuckets)
                .bind(Types.StringType.get())
                .apply(partitionValue.toStringUtf8());
    }

    @ScalarFunction(schema = "system", value = "bucket")
    @Description("The Iceberg bucket transform")
    @SqlType(INTEGER)
    public static long bucket(@SqlType(INTEGER) long partitionValue, @SqlType(INTEGER) long numberOfBuckets)
    {
        return Transforms.bucket((int) numberOfBuckets)
                .bind(Types.IntegerType.get())
                .apply((int) partitionValue);
    }

    @ScalarFunction(schema = "system", value = "bucket")
    @Description("The Iceberg bucket transform")
    @SqlType(INTEGER)
    public static long bucket(@SqlNullable @SqlType(StandardTypes.BIGINT) Long partitionValue, @SqlType(INTEGER) long numberOfBuckets)
    {
        return Transforms.bucket((int) numberOfBuckets)
                .bind(Types.LongType.get())
                .apply(partitionValue);
    }

    @LiteralParameters({"p", "s"})
    @ScalarFunction(schema = "system", value = "bucket")
    @Description("The Iceberg bucket transform")
    @SqlType(INTEGER)
    public static long bucket(@LiteralParameter("p") long p, @LiteralParameter("s") long s, @SqlType("decimal(p, s)") Object partitionValue, @SqlType(INTEGER) long numberOfBuckets)
    {
        BigInteger unscaledValue;
        if (p <= MAX_SHORT_PRECISION) {
            unscaledValue = BigInteger.valueOf((Long) partitionValue);
        }
        else {
            unscaledValue = ((Int128) partitionValue).toBigInteger();
        }
        return Transforms.bucket((int) numberOfBuckets)
                .bind(Types.DecimalType.of((int) p, (int) s))
                .apply(new BigDecimal(unscaledValue));
    }

    @LiteralParameters("p")
    @ScalarFunction(schema = "system", value = "bucket")
    @Description("The Iceberg bucket transform")
    @SqlType(INTEGER)
    public static long bucketTimestamp(@SqlType("timestamp(p) with time zone") long partitionValue, @SqlType(INTEGER) long numberOfBuckets)
    {
        return Transforms.bucket((int) numberOfBuckets)
                .bind(Types.TimestampType.withZone())
                .apply(partitionValue);
    }

    @ScalarFunction(schema = "system", value = "bucket")
    @Description("The Iceberg bucket transform")
    @SqlType(INTEGER)
    public static long bucketDate(@SqlType(DATE) long partitionValue, @SqlType(INTEGER) long numberOfBuckets)
    {
        // DATE type in Iceberg uses BucketInteger transform which expects Integer value
        return Transforms.bucket((int) numberOfBuckets)
                .bind(Types.DateType.get())
                .apply((int) partitionValue);
    }
}
