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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static java.lang.String.format;

public class ProtocolEntry
{
    private static final int MIN_VERSION_SUPPORTS_READER_FEATURES = 3;
    private static final int MIN_VERSION_SUPPORTS_WRITER_FEATURES = 7;

    private final int minReaderVersion;
    private final int minWriterVersion;
    private final Optional<Set<String>> readerFeatures;
    private final Optional<Set<String>> writerFeatures;

    @JsonCreator
    public ProtocolEntry(
            @JsonProperty("minReaderVersion") int minReaderVersion,
            @JsonProperty("minWriterVersion") int minWriterVersion,
            // The delta protocol documentation mentions that readerFeatures & writerFeatures is Array[String], but their actual implementation is Set
            @JsonProperty("readerFeatures") Optional<Set<String>> readerFeatures,
            @JsonProperty("writerFeatures") Optional<Set<String>> writerFeatures)
    {
        this.minReaderVersion = minReaderVersion;
        this.minWriterVersion = minWriterVersion;
        if (minReaderVersion < MIN_VERSION_SUPPORTS_READER_FEATURES && readerFeatures.isPresent()) {
            throw new IllegalArgumentException("readerFeatures must not exist when minReaderVersion is less than " + MIN_VERSION_SUPPORTS_READER_FEATURES);
        }
        if (minWriterVersion < MIN_VERSION_SUPPORTS_WRITER_FEATURES && writerFeatures.isPresent()) {
            throw new IllegalArgumentException("writerFeatures must not exist when minWriterVersion is less than " + MIN_VERSION_SUPPORTS_WRITER_FEATURES);
        }
        this.readerFeatures = readerFeatures;
        this.writerFeatures = writerFeatures;
    }

    @JsonProperty
    public int getMinReaderVersion()
    {
        return minReaderVersion;
    }

    @JsonProperty
    public int getMinWriterVersion()
    {
        return minWriterVersion;
    }

    @JsonProperty
    public Optional<Set<String>> getReaderFeatures()
    {
        return readerFeatures;
    }

    @JsonProperty
    public Optional<Set<String>> getWriterFeatures()
    {
        return writerFeatures;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ProtocolEntry that = (ProtocolEntry) o;
        return minReaderVersion == that.minReaderVersion &&
                minWriterVersion == that.minWriterVersion &&
                readerFeatures.equals(that.readerFeatures) &&
                writerFeatures.equals(that.writerFeatures);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(minReaderVersion, minWriterVersion, readerFeatures, writerFeatures);
    }

    @Override
    public String toString()
    {
        return format(
                "ProtocolEntry{minReaderVersion=%d, minWriterVersion=%d, readerFeatures=%s, writerFeatures=%s}",
                minReaderVersion,
                minWriterVersion,
                readerFeatures,
                writerFeatures);
    }
}
