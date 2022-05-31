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

import static java.lang.String.format;

public class ProtocolEntry
{
    private final int minReaderVersion;
    private final int minWriterVersion;

    @JsonCreator
    public ProtocolEntry(
            @JsonProperty("minReaderVersion") int minReaderVersion,
            @JsonProperty("minWriterVersion") int minWriterVersion)
    {
        this.minReaderVersion = minReaderVersion;
        this.minWriterVersion = minWriterVersion;
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
                minWriterVersion == that.minWriterVersion;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(minReaderVersion, minWriterVersion);
    }

    @Override
    public String toString()
    {
        return format("ProtocolEntry{minReaderVersion=%d, minWriterVersion=%d}", minReaderVersion, minWriterVersion);
    }
}
