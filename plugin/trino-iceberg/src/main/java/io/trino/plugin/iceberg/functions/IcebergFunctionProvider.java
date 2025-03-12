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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.trino.plugin.base.classloader.ClassLoaderSafeTableFunctionProcessorProviderFactory;
import io.trino.plugin.iceberg.functions.tablechanges.TableChangesFunctionHandle;
import io.trino.plugin.iceberg.functions.tablechanges.TableChangesFunctionProcessorProviderFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.ScalarFunctionAdapter;
import io.trino.spi.function.ScalarFunctionImplementation;
import io.trino.spi.function.Signature;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionProcessorProviderFactory;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;

import java.lang.invoke.MethodHandle;
import java.math.BigDecimal;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.iceberg.util.Timestamps.timestampTzToMicros;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

public class IcebergFunctionProvider
        implements FunctionProvider
{
    public static final List<FunctionMetadata> FUNCTIONS = ImmutableList.<FunctionMetadata>builder()
            .add(FunctionMetadata.scalarBuilder("bucket")
                    .functionId(new FunctionId("bucket"))
                    .nondeterministic()
                    .description("Perform Iceberg bucket transform")
                    .signature(Signature.builder()
                            .typeVariable("T")
                            .returnType(INTEGER.getTypeSignature())
                            .argumentTypes(ImmutableList.of(new TypeSignature("T"), INTEGER.getTypeSignature()))
                            .build())
                    .nullable()
                    .build())
            .build();

    private static final MethodHandle BUCKET_INTEGER;
    private static final MethodHandle BUCKET_SHORT_DECIMAL;
    private static final MethodHandle BUCKET_LONG_DECIMAL;
    private static final MethodHandle BUCKET_VARCHAR;
    private static final MethodHandle BUCKET_VARBINARY;
    private static final MethodHandle BUCKET_DATE;
    private static final MethodHandle BUCKET_SHORT_TIMESTAMP;
    private static final MethodHandle BUCKET_LONG_TIMESTAMP;
    private static final MethodHandle BUCKET_SHORT_TIMESTAMP_WITH_TIME_ZONE;
    private static final MethodHandle BUCKET_LONG_TIMESTAMP_WITH_TIME_ZONE;

    static {
        try {
            BUCKET_INTEGER = lookup().findVirtual(IcebergFunctionProvider.class, "bucketInteger", methodType(long.class, long.class, long.class));
            BUCKET_SHORT_DECIMAL = lookup().findVirtual(IcebergFunctionProvider.class, "bucketShortDecimal", methodType(long.class, DecimalType.class, long.class, long.class));
            BUCKET_LONG_DECIMAL = lookup().findVirtual(IcebergFunctionProvider.class, "bucketLongDecimal", methodType(long.class, DecimalType.class, Int128.class, long.class));
            BUCKET_VARCHAR = lookup().findVirtual(IcebergFunctionProvider.class, "bucketVarchar", methodType(long.class, Slice.class, long.class));
            BUCKET_VARBINARY = lookup().findVirtual(IcebergFunctionProvider.class, "bucketVarbinary", methodType(long.class, Slice.class, long.class));
            BUCKET_DATE = lookup().findVirtual(IcebergFunctionProvider.class, "bucketDate", methodType(long.class, long.class, long.class));
            BUCKET_SHORT_TIMESTAMP = lookup().findVirtual(IcebergFunctionProvider.class, "bucketShortTimestamp", methodType(long.class, long.class, long.class));
            BUCKET_LONG_TIMESTAMP = lookup().findVirtual(IcebergFunctionProvider.class, "bucketLongTimestamp", methodType(long.class, LongTimestamp.class, long.class));
            BUCKET_SHORT_TIMESTAMP_WITH_TIME_ZONE = lookup().findVirtual(IcebergFunctionProvider.class, "bucketShortTimestampWithTimeZone", methodType(long.class, long.class, long.class));
            BUCKET_LONG_TIMESTAMP_WITH_TIME_ZONE = lookup().findVirtual(IcebergFunctionProvider.class, "bucketLongTimestampWithTimeZone", methodType(long.class, LongTimestampWithTimeZone.class, long.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final TableChangesFunctionProcessorProviderFactory tableChangesFunctionProcessorProviderFactory;

    @Inject
    public IcebergFunctionProvider(TableChangesFunctionProcessorProviderFactory tableChangesFunctionProcessorProviderFactory)
    {
        this.tableChangesFunctionProcessorProviderFactory = requireNonNull(tableChangesFunctionProcessorProviderFactory, "tableChangesFunctionProcessorProviderFactory is null");
    }

    @Override
    public ScalarFunctionImplementation getScalarFunctionImplementation(
            FunctionId functionId,
            BoundSignature boundSignature,
            FunctionDependencies functionDependencies,
            InvocationConvention invocationConvention)
    {
        List<Type> argumentTypes = boundSignature.getArgumentTypes();
        checkArgument(argumentTypes.size() == 2, "Expected two arguments, but got %s", argumentTypes.size());
        checkArgument(argumentTypes.getLast() == INTEGER, "The 2nd argument must be integer type, but got %s", argumentTypes.getLast());
        Type type = argumentTypes.getFirst();

        MethodHandle handle = switch (type) {
            case TinyintType _, SmallintType _, IntegerType _, BigintType _ -> BUCKET_INTEGER;
            case DecimalType decimalType -> decimalType.isShort() ? BUCKET_SHORT_DECIMAL : BUCKET_LONG_DECIMAL;
            case VarcharType _ -> BUCKET_VARCHAR;
            case VarbinaryType _ -> BUCKET_VARBINARY;
            case DateType _ -> BUCKET_DATE;
            case TimestampType timestampType -> timestampType.isShort() ? BUCKET_SHORT_TIMESTAMP : BUCKET_LONG_TIMESTAMP;
            case TimestampWithTimeZoneType timestampWithTimeZoneType -> timestampWithTimeZoneType.isShort() ? BUCKET_SHORT_TIMESTAMP_WITH_TIME_ZONE : BUCKET_LONG_TIMESTAMP_WITH_TIME_ZONE;
            default -> throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Unsupported type: " + type);
        };

        handle = handle.bindTo(this);

        if (type instanceof DecimalType decimalType) {
            handle = handle.bindTo(decimalType);
        }

        InvocationConvention actualConvention = new InvocationConvention(
                nCopies(boundSignature.getArity(), NEVER_NULL),
                FAIL_ON_NULL,
                false,
                false);

        handle = ScalarFunctionAdapter.adapt(
                handle,
                boundSignature.getReturnType(),
                boundSignature.getArgumentTypes(),
                actualConvention,
                invocationConvention);

        return ScalarFunctionImplementation.builder()
                .methodHandle(handle)
                .build();
    }

    @SuppressWarnings("MethodMayBeStatic")
    public long bucketInteger(long value, long numberOfBuckets)
    {
        return Transforms.bucket((int) numberOfBuckets)
                .bind(Types.LongType.get())
                .apply(value);
    }

    @SuppressWarnings("MethodMayBeStatic")
    public long bucketShortDecimal(DecimalType decimalType, long value, long numberOfBuckets)
    {
        return Transforms.bucket((int) numberOfBuckets)
                .bind(Types.DecimalType.of(decimalType.getPrecision(), decimalType.getScale()))
                .apply(BigDecimal.valueOf(value));
    }

    @SuppressWarnings("MethodMayBeStatic")
    public long bucketLongDecimal(DecimalType decimalType, Int128 value, long numberOfBuckets)
    {
        return Transforms.bucket((int) numberOfBuckets)
                .bind(Types.DecimalType.of(decimalType.getPrecision(), decimalType.getScale()))
                .apply(new BigDecimal(value.toBigInteger()));
    }

    @SuppressWarnings("MethodMayBeStatic")
    public long bucketVarchar(Slice value, long numberOfBuckets)
    {
        return (long) Transforms.bucket((int) numberOfBuckets)
                .bind(Types.StringType.get())
                .apply(value.toStringUtf8());
    }

    @SuppressWarnings("MethodMayBeStatic")
    public long bucketVarbinary(Slice value, long numberOfBuckets)
    {
        return (long) Transforms.bucket((int) numberOfBuckets)
                .bind(Types.BinaryType.get())
                .apply(value.toByteBuffer());
    }

    @SuppressWarnings("MethodMayBeStatic")
    public long bucketDate(long value, long numberOfBuckets)
    {
        return Transforms.bucket((int) numberOfBuckets)
                .bind(Types.DateType.get())
                .apply((int) value);
    }

    @SuppressWarnings("MethodMayBeStatic")
    public long bucketShortTimestamp(long value, long numberOfBuckets)
    {
        return Transforms.bucket((int) numberOfBuckets)
                .bind(Types.TimestampType.withoutZone())
                .apply(value);
    }

    @SuppressWarnings("MethodMayBeStatic")
    public long bucketLongTimestamp(LongTimestamp value, long numberOfBuckets)
    {
        return Transforms.bucket((int) numberOfBuckets)
                .bind(Types.TimestampType.withoutZone())
                .apply(value.getEpochMicros());
    }

    @SuppressWarnings("MethodMayBeStatic")
    public long bucketShortTimestampWithTimeZone(long value, long numberOfBuckets)
    {
        return Transforms.bucket((int) numberOfBuckets)
                .bind(Types.TimestampType.withZone())
                .apply(unpackMillisUtc(value) * MICROSECONDS_PER_MILLISECOND);
    }

    @SuppressWarnings("MethodMayBeStatic")
    public long bucketLongTimestampWithTimeZone(LongTimestampWithTimeZone value, long numberOfBuckets)
    {
        return Transforms.bucket((int) numberOfBuckets)
                .bind(Types.TimestampType.withZone())
                .apply(timestampTzToMicros(value));
    }

    @Override
    public TableFunctionProcessorProviderFactory getTableFunctionProcessorProviderFactory(ConnectorTableFunctionHandle functionHandle)
    {
        if (functionHandle instanceof TableChangesFunctionHandle) {
            return new ClassLoaderSafeTableFunctionProcessorProviderFactory(tableChangesFunctionProcessorProviderFactory, getClass().getClassLoader());
        }

        throw new UnsupportedOperationException("Unsupported function: " + functionHandle);
    }
}
