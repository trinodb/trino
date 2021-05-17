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
package io.trino.parquet.reader;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.multibindings.Multibinder;
import io.trino.parquet.RichColumnDescriptor;
import io.trino.spi.TrinoException;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.joda.time.DateTimeZone;

import javax.annotation.Nullable;

import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.parquet.ParquetTypeUtils.createDecimalType;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.OriginalType.TIMESTAMP_MICROS;
import static org.apache.parquet.schema.OriginalType.TIMESTAMP_MILLIS;
import static org.apache.parquet.schema.OriginalType.TIME_MICROS;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;

public class ColumnReaderFactory
{
    private final Set<ColumnReaderProvider> readerProviders;

    @Inject
    public ColumnReaderFactory(Set<ColumnReaderProvider> readerProviders)
    {
        this.readerProviders = requireNonNull(readerProviders, "readerProviders is null");
    }

    public ColumnReader create(RichColumnDescriptor descriptor, DateTimeZone timeZone)
    {
        int highestPriority = Integer.MIN_VALUE;
        @Nullable
        ColumnReaderProvider candidate = null;

        for (ColumnReaderProvider provider : readerProviders.stream()
                .filter(provider -> provider.accepts(descriptor, timeZone))
                .collect(toImmutableSet())) {
            int priority = provider.priority();
            if (priority == highestPriority) {
                throw new TrinoException(ALREADY_EXISTS,
                        "Found two matching column readers with the same priority " + priority + " for key "
                                + descriptor.getPrimitiveType().getPrimitiveTypeName());
            }
            if (priority > highestPriority) {
                highestPriority = priority;
                candidate = provider;
            }
        }

        if (candidate == null) {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported parquet type: " + descriptor.getPrimitiveType().getPrimitiveTypeName());
        }
        return candidate.create(descriptor, timeZone);
    }

    private static Optional<PrimitiveColumnReader> createDecimalColumnReader(RichColumnDescriptor descriptor)
    {
        return createDecimalType(descriptor)
                .map(decimalType -> DecimalColumnReaderFactory.createReader(descriptor, decimalType));
    }

    public static class ColumnReaderModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            Multibinder<ColumnReaderProvider> readerProviders = Multibinder.newSetBinder(binder, ColumnReaderProvider.class);
            readerProviders.addBinding().toInstance(simpleColumnReaderProvider(BOOLEAN, BooleanColumnReader::new));
            readerProviders.addBinding().toInstance(simpleColumnReaderProvider(INT32, descriptor -> createDecimalColumnReader(descriptor).orElse(new IntColumnReader(descriptor))));
            readerProviders.addBinding().toInstance(timestampColumnReaderProvider(TIME_MICROS, TimeMicrosColumnReader::new));
            readerProviders.addBinding().toInstance(timestampColumnReaderProvider(TIMESTAMP_MICROS, TimestampMicrosColumnReader::new));
            readerProviders.addBinding().toInstance(timestampColumnReaderProvider(TIMESTAMP_MILLIS, Int64TimestampMillisColumnReader::new));
            readerProviders.addBinding().toInstance(simpleColumnReaderProvider(INT64, descriptor -> createDecimalColumnReader(descriptor).orElse(new LongColumnReader(descriptor))));
            readerProviders.addBinding().toInstance(simpleColumnReaderProvider(INT96, TimestampColumnReader::new));
            readerProviders.addBinding().toInstance(simpleColumnReaderProvider(FLOAT, FloatColumnReader::new));
            readerProviders.addBinding().toInstance(simpleColumnReaderProvider(DOUBLE, DoubleColumnReader::new));
            readerProviders.addBinding().toInstance(simpleColumnReaderProvider(BINARY, descriptor -> createDecimalColumnReader(descriptor).orElse(new BinaryColumnReader(descriptor))));
            readerProviders.addBinding().toInstance(simpleColumnReaderProvider(FIXED_LEN_BYTE_ARRAY, descriptor -> createDecimalColumnReader(descriptor)
                    .orElseThrow(() -> new TrinoException(NOT_SUPPORTED, " type FIXED_LEN_BYTE_ARRAY supported as DECIMAL; got " + descriptor.getPrimitiveType().getOriginalType()))));
        }
    }

    public static ColumnReaderProvider simpleColumnReaderProvider(PrimitiveTypeName typeName, Function<RichColumnDescriptor, ColumnReader> reader)
    {
        return simpleColumnReaderProvider(typeName, ((descriptor, dateTimeZone) -> reader.apply(descriptor)));
    }

    public static ColumnReaderProvider timestampColumnReaderProvider(OriginalType type, Function<RichColumnDescriptor, ColumnReader> reader)
    {
        return new ColumnReaderProvider()
        {
            @Override
            public boolean accepts(RichColumnDescriptor descriptor, DateTimeZone timeZone)
            {
                return descriptor.getPrimitiveType().getPrimitiveTypeName() == INT64
                        && descriptor.getPrimitiveType().getOriginalType() == type;
            }

            @Override
            public ColumnReader create(RichColumnDescriptor descriptor, DateTimeZone timeZone)
            {
                return reader.apply(descriptor);
            }

            @Override
            public int priority()
            {
                return 10;
            }
        };
    }

    public static ColumnReaderProvider simpleColumnReaderProvider(
            PrimitiveTypeName typeName,
            BiFunction<RichColumnDescriptor, DateTimeZone, ColumnReader> reader)
    {
        return new ColumnReaderProvider()
        {
            @Override
            public boolean accepts(RichColumnDescriptor descriptor, DateTimeZone timeZone)
            {
                return descriptor.getPrimitiveType().getPrimitiveTypeName() == typeName;
            }

            @Override
            public ColumnReader create(RichColumnDescriptor descriptor, DateTimeZone timeZone)
            {
                return reader.apply(descriptor, timeZone);
            }
        };
    }

    public interface ColumnReaderProvider
    {
        boolean accepts(RichColumnDescriptor descriptor, DateTimeZone timeZone);

        ColumnReader create(RichColumnDescriptor descriptor, DateTimeZone timeZone);

        // Higher are first
        default int priority()
        {
            return 0;
        }
    }
}
