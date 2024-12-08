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
import static io.trino.spi.type.StandardTypes.VARCHAR;

public final class IcebergBucketFunction
{
    private IcebergBucketFunction() {}

    @Description("A UDF for the Iceberg bucket transform.")
    @ScalarFunction("iceberg_bucket")
    @SqlType(StandardTypes.INTEGER)
    public static long icebergBucket(
            @SqlNullable @SqlType(VARCHAR) Slice partitionValue,
            @SqlNullable @SqlType(StandardTypes.INTEGER) Long numberOfBuckets)
    {
        return Transforms.bucket(numberOfBuckets.intValue()).bind(Types.StringType.get()).apply(partitionValue.toStringUtf8());
    }

    @Description("A UDF for the Iceberg bucket transform.")
    @ScalarFunction("iceberg_bucket")
    @SqlType(StandardTypes.INTEGER)
    public static long icebergBucket(
            @SqlType(StandardTypes.INTEGER) long partitionValue,
            @SqlNullable @SqlType(StandardTypes.INTEGER) Long numberOfBuckets)
    {
        return Transforms.bucket(numberOfBuckets.intValue()).bind(Types.IntegerType.get()).apply((int) partitionValue);
    }

    @Description("A UDF for the Iceberg bucket transform.")
    @ScalarFunction("iceberg_bucket")
    @SqlType(StandardTypes.INTEGER)
    public static long icebergBucket(
            @SqlNullable @SqlType(StandardTypes.BIGINT) Long partitionValue,
            @SqlNullable @SqlType(StandardTypes.INTEGER) Long numberOfBuckets)
    {
        return Transforms.bucket(numberOfBuckets.intValue()).bind(Types.LongType.get()).apply(partitionValue);
    }

    @LiteralParameters({"p", "s"})
    @Description("A UDF for the Iceberg bucket transform.")
    @ScalarFunction("iceberg_bucket")
    @SqlType(StandardTypes.INTEGER)
    public static long icebergBucketDecimal(
            @LiteralParameter("p") long p,
            @LiteralParameter("s") long s,
            @SqlType("decimal(p, s)") Object partitionValue,
            @SqlNullable @SqlType(StandardTypes.INTEGER) Long numberOfBuckets)
    {
        BigInteger unscaledValue;
        if (p <= MAX_SHORT_PRECISION) {
            unscaledValue = BigInteger.valueOf((Long) partitionValue);
        }
        else {
            unscaledValue = ((Int128) partitionValue).toBigInteger();
        }
        return Transforms
                .bucket(numberOfBuckets.intValue())
                .bind(Types.DecimalType.of((int) p, (int) s))
                .apply(new BigDecimal(unscaledValue));
    }

    @LiteralParameters("p")
    @Description("A UDF for the Iceberg bucket transform.")
    @ScalarFunction("iceberg_bucket")
    @SqlType(StandardTypes.INTEGER)
    public static long icebergBucketTimestamp(
            @SqlType("timestamp(p) with time zone") long partitionValue,
            @SqlNullable @SqlType(StandardTypes.INTEGER) Long numberOfBuckets)
    {
        return Transforms.bucket(numberOfBuckets.intValue()).bind(Types.TimestampType.withZone()).apply(partitionValue);
    }

    @Description("A UDF for the Iceberg bucket transform.")
    @ScalarFunction("iceberg_bucket")
    @SqlType(StandardTypes.INTEGER)
    public static long icebergBucketDate(
            @SqlType(StandardTypes.DATE) long partitionValue,
            @SqlNullable @SqlType(StandardTypes.INTEGER) Long numberOfBuckets)
    {
        // DATE type in Iceberg uses BucketInteger transform which
        // expects Integer value
        return Transforms.bucket(numberOfBuckets.intValue()).bind(Types.DateType.get()).apply((int) partitionValue);
    }
}
