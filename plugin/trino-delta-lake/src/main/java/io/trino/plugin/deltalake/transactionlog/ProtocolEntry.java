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
package io.trino.plugin.deltalake.transactionlog;

import com.google.common.collect.ImmutableSet;
import io.airlift.slice.SizeOf;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static com.google.common.primitives.Ints.max;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.plugin.deltalake.DeltaLakeMetadata.CDF_SUPPORTED_WRITER_VERSION;
import static io.trino.plugin.deltalake.DeltaLakeMetadata.COLUMN_MAPPING_MODE_SUPPORTED_READER_VERSION;
import static io.trino.plugin.deltalake.DeltaLakeMetadata.COLUMN_MAPPING_MODE_SUPPORTED_WRITER_VERSION;
import static io.trino.plugin.deltalake.DeltaLakeMetadata.DELETION_VECTORS_SUPPORTED_READER_VERSION;
import static io.trino.plugin.deltalake.DeltaLakeMetadata.DELETION_VECTORS_SUPPORTED_WRITER_VERSION;
import static io.trino.plugin.deltalake.DeltaLakeMetadata.TIMESTAMP_NTZ_SUPPORTED_READER_VERSION;
import static io.trino.plugin.deltalake.DeltaLakeMetadata.TIMESTAMP_NTZ_SUPPORTED_WRITER_VERSION;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeTableFeatures.CHANGE_DATA_FEED_FEATURE_NAME;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeTableFeatures.COLUMN_MAPPING_FEATURE_NAME;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeTableFeatures.DELETION_VECTORS_FEATURE_NAME;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeTableFeatures.MIN_VERSION_SUPPORTS_READER_FEATURES;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeTableFeatures.MIN_VERSION_SUPPORTS_WRITER_FEATURES;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeTableFeatures.TIMESTAMP_NTZ_FEATURE_NAME;
import static java.util.Objects.requireNonNull;

public record ProtocolEntry(
        int minReaderVersion,
        int minWriterVersion,
        // The delta protocol documentation mentions that readerFeatures & writerFeatures is Array[String], but their actual implementation is Set
        Optional<Set<String>> readerFeatures,
        Optional<Set<String>> writerFeatures)
{
    private static final int INSTANCE_SIZE = instanceSize(ProtocolEntry.class);

    public ProtocolEntry
    {
        if (minReaderVersion < MIN_VERSION_SUPPORTS_READER_FEATURES && readerFeatures.isPresent()) {
            throw new IllegalArgumentException("readerFeatures must not exist when minReaderVersion is less than " + MIN_VERSION_SUPPORTS_READER_FEATURES);
        }
        if (minWriterVersion < MIN_VERSION_SUPPORTS_WRITER_FEATURES && writerFeatures.isPresent()) {
            throw new IllegalArgumentException("writerFeatures must not exist when minWriterVersion is less than " + MIN_VERSION_SUPPORTS_WRITER_FEATURES);
        }
        readerFeatures = requireNonNull(readerFeatures, "readerFeatures is null").map(ImmutableSet::copyOf);
        writerFeatures = requireNonNull(writerFeatures, "writerFeatures is null").map(ImmutableSet::copyOf);
    }

    public boolean supportsReaderFeatures()
    {
        return minReaderVersion >= MIN_VERSION_SUPPORTS_READER_FEATURES;
    }

    public boolean readerFeaturesContains(String featureName)
    {
        return readerFeatures.map(features -> features.contains(featureName)).orElse(false);
    }

    public boolean supportsWriterFeatures()
    {
        return minWriterVersion >= MIN_VERSION_SUPPORTS_WRITER_FEATURES;
    }

    public boolean writerFeaturesContains(String featureName)
    {
        return writerFeatures.map(features -> features.contains(featureName)).orElse(false);
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + SIZE_OF_INT
                + SIZE_OF_INT
                + sizeOf(readerFeatures, features -> estimatedSizeOf(features, SizeOf::estimatedSizeOf))
                + sizeOf(writerFeatures, features -> estimatedSizeOf(features, SizeOf::estimatedSizeOf));
    }

    public static Builder builder(ProtocolEntry protocolEntry)
    {
        return new Builder(protocolEntry.minReaderVersion, protocolEntry.minWriterVersion, protocolEntry.readerFeatures, protocolEntry.writerFeatures);
    }

    public static Builder builder(int readerVersion, int writerVersion)
    {
        return new Builder(readerVersion, writerVersion, Optional.empty(), Optional.empty());
    }

    public static class Builder
    {
        private int readerVersion;
        private int writerVersion;
        private final Set<String> readerFeatures = new HashSet<>();
        private final Set<String> writerFeatures = new HashSet<>();

        private Builder(int readerVersion, int writerVersion, Optional<Set<String>> readerFeatures, Optional<Set<String>> writerFeatures)
        {
            this.readerVersion = readerVersion;
            this.writerVersion = writerVersion;
            requireNonNull(readerFeatures, "readerFeatures is null").ifPresent(this.readerFeatures::addAll);
            requireNonNull(writerFeatures, "writerFeatures is null").ifPresent(this.writerFeatures::addAll);
        }

        public Builder enableChangeDataFeed()
        {
            writerVersion = max(writerVersion, CDF_SUPPORTED_WRITER_VERSION);
            writerFeatures.add(CHANGE_DATA_FEED_FEATURE_NAME);
            return this;
        }

        public Builder enableColumnMapping()
        {
            readerVersion = max(readerVersion, COLUMN_MAPPING_MODE_SUPPORTED_READER_VERSION);
            writerVersion = max(writerVersion, COLUMN_MAPPING_MODE_SUPPORTED_WRITER_VERSION);
            readerFeatures.add(COLUMN_MAPPING_FEATURE_NAME);
            writerFeatures.add(COLUMN_MAPPING_FEATURE_NAME);
            return this;
        }

        public Builder enableTimestampNtz()
        {
            readerVersion = max(readerVersion, TIMESTAMP_NTZ_SUPPORTED_READER_VERSION);
            writerVersion = max(writerVersion, TIMESTAMP_NTZ_SUPPORTED_WRITER_VERSION);
            readerFeatures.add(TIMESTAMP_NTZ_FEATURE_NAME);
            writerFeatures.add(TIMESTAMP_NTZ_FEATURE_NAME);
            return this;
        }

        public Builder enableDeletionVector()
        {
            readerVersion = max(readerVersion, DELETION_VECTORS_SUPPORTED_READER_VERSION);
            writerVersion = max(writerVersion, DELETION_VECTORS_SUPPORTED_WRITER_VERSION);
            readerFeatures.add(DELETION_VECTORS_FEATURE_NAME);
            writerFeatures.add(DELETION_VECTORS_FEATURE_NAME);
            return this;
        }

        public ProtocolEntry build()
        {
            return new ProtocolEntry(
                    readerVersion,
                    writerVersion,
                    readerVersion < MIN_VERSION_SUPPORTS_READER_FEATURES || readerFeatures.isEmpty() ? Optional.empty() : Optional.of(readerFeatures),
                    writerVersion < MIN_VERSION_SUPPORTS_WRITER_FEATURES || writerFeatures.isEmpty() ? Optional.empty() : Optional.of(writerFeatures));
        }
    }
}
