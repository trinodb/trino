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
package io.trino.plugin.iceberg.delete;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.predicate.TupleDomain;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.assertj.core.api.Assertions.assertThat;

class TestSmallFileAwareDeletePageSourceProvider
{
    private static final long ONE_MEGABYTE = 1024 * 1024;

    private final SmallFileAwareDeletePageSourceProvider provider = new SmallFileAwareDeletePageSourceProvider(
            (_, _, _) -> new FixedPageSource(ImmutableList.of()),
            DataSize.of(3, MEGABYTE),
            DataSize.of(8, MEGABYTE));

    @Test
    void testParquetUsesParquetThreshold()
    {
        assertThat(provider.estimateOpenedBytes(file(FileFormat.PARQUET, 3 * ONE_MEGABYTE))).isEqualTo(3 * ONE_MEGABYTE);
        assertThat(provider.estimateOpenedBytes(file(FileFormat.PARQUET, 3 * ONE_MEGABYTE + 1))).isEqualTo(0);
    }

    @Test
    void testOrcUsesTinyStripeThreshold()
    {
        assertThat(provider.estimateOpenedBytes(file(FileFormat.ORC, 8 * ONE_MEGABYTE))).isEqualTo(8 * ONE_MEGABYTE);
        assertThat(provider.estimateOpenedBytes(file(FileFormat.ORC, 8 * ONE_MEGABYTE + 1))).isEqualTo(0);
        // would be over the Parquet threshold
        assertThat(provider.estimateOpenedBytes(file(FileFormat.ORC, 5 * ONE_MEGABYTE))).isEqualTo(5 * ONE_MEGABYTE);
    }

    @Test
    void testAvroIsNotBufferedWhenOpened()
    {
        assertThat(provider.estimateOpenedBytes(file(FileFormat.AVRO, 1024))).isEqualTo(0);
        assertThat(provider.estimateOpenedBytes(file(FileFormat.AVRO, 100 * ONE_MEGABYTE))).isEqualTo(0);
    }

    @Test
    void testConfiguredThresholdsAreUsed()
    {
        SmallFileAwareDeletePageSourceProvider custom = new SmallFileAwareDeletePageSourceProvider(
                (_, _, _) -> new FixedPageSource(ImmutableList.of()),
                DataSize.of(1, MEGABYTE),
                DataSize.of(16, MEGABYTE));

        assertThat(custom.estimateOpenedBytes(file(FileFormat.PARQUET, 2 * ONE_MEGABYTE))).isEqualTo(0);
        assertThat(custom.estimateOpenedBytes(file(FileFormat.ORC, 12 * ONE_MEGABYTE))).isEqualTo(12 * ONE_MEGABYTE);
    }

    @Test
    void testOpenDelegates()
    {
        AtomicInteger opened = new AtomicInteger();
        ConnectorPageSource pageSource = new FixedPageSource(ImmutableList.of());
        SmallFileAwareDeletePageSourceProvider delegating = new SmallFileAwareDeletePageSourceProvider(
                (_, _, _) -> {
                    opened.incrementAndGet();
                    return pageSource;
                },
                DataSize.of(3, MEGABYTE),
                DataSize.of(8, MEGABYTE));

        assertThat(delegating.openDeletes(file(FileFormat.PARQUET, 1024), ImmutableList.of(), TupleDomain.all())).isSameAs(pageSource);
        assertThat(opened).hasValue(1);
    }

    private static DeleteFile file(FileFormat format, long sizeInBytes)
    {
        return new DeleteFile(
                FileContent.POSITION_DELETES,
                "s3://bucket/delete-file",
                format,
                1,
                sizeInBytes,
                ImmutableList.of(),
                OptionalLong.empty(),
                OptionalLong.empty(),
                1,
                OptionalLong.empty(),
                Optional.empty(),
                Optional.empty());
    }
}
