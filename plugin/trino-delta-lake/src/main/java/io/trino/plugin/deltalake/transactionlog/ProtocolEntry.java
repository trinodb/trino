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

import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public record ProtocolEntry(
        int minReaderVersion,
        int minWriterVersion,
        // The delta protocol documentation mentions that readerFeatures & writerFeatures is Array[String], but their actual implementation is Set
        Optional<Set<String>> readerFeatures,
        Optional<Set<String>> writerFeatures)
{
    private static final int MIN_VERSION_SUPPORTS_READER_FEATURES = 3;
    private static final int MIN_VERSION_SUPPORTS_WRITER_FEATURES = 7;

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
}
