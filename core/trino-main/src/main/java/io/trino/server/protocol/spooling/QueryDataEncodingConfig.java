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
package io.trino.server.protocol.spooling;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDataSize;

import static io.airlift.units.DataSize.Unit.KILOBYTE;

public class QueryDataEncodingConfig
{
    private boolean jsonEnabled = true;
    private boolean jsonZstdEnabled = true;
    private boolean jsonLz4Enabled = true;
    private DataSize compressionThreshold = DataSize.of(8, KILOBYTE);

    public boolean isJsonEnabled()
    {
        return jsonEnabled;
    }

    @Config("protocol.spooling.encoding.json.enabled")
    @ConfigDescription("Enable uncompressed json spooled encoding")
    public QueryDataEncodingConfig setJsonEnabled(boolean jsonEnabled)
    {
        this.jsonEnabled = jsonEnabled;
        return this;
    }

    public boolean isJsonZstdEnabled()
    {
        return jsonZstdEnabled;
    }

    @Config("protocol.spooling.encoding.json+zstd.enabled")
    @ConfigDescription("Enable Zstd compressed json spooled encoding")
    public QueryDataEncodingConfig setJsonZstdEnabled(boolean jsonZstdEnabled)
    {
        this.jsonZstdEnabled = jsonZstdEnabled;
        return this;
    }

    public boolean isJsonLz4Enabled()
    {
        return jsonLz4Enabled;
    }

    @Config("protocol.spooling.encoding.json+lz4.enabled")
    @ConfigDescription("Enable LZ4 compressed json spooled encoding")
    public QueryDataEncodingConfig setJsonLz4Enabled(boolean jsonLz4Enabled)
    {
        this.jsonLz4Enabled = jsonLz4Enabled;
        return this;
    }

    @MinDataSize("1kB")
    @MaxDataSize("4MB")
    public DataSize getCompressionThreshold()
    {
        return compressionThreshold;
    }

    @Config("protocol.spooling.encoding.compression.threshold")
    @ConfigDescription("Do not compress segments smaller than threshold")
    public QueryDataEncodingConfig setCompressionThreshold(DataSize compressionThreshold)
    {
        this.compressionThreshold = compressionThreshold;
        return this;
    }
}
