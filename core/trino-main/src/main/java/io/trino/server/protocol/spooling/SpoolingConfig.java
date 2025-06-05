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
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.LegacyConfig;
import io.airlift.units.DataSize;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDataSize;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.util.Ciphers.is256BitSecretKeySpec;
import static java.util.Base64.getDecoder;

public class SpoolingConfig
{
    private SecretKey sharedSecretKey;
    private SegmentRetrievalMode retrievalMode = SegmentRetrievalMode.STORAGE;

    private boolean inliningEnabled = true;
    private long inliningMaxRows = 50_000;
    private DataSize inliningMaxSize = DataSize.of(3, MEGABYTE);
    private DataSize initialSegmentSize = DataSize.of(8, MEGABYTE);
    private DataSize maximumSegmentSize = DataSize.of(16, MEGABYTE);

    @NotNull
    public SecretKey getSharedSecretKey()
    {
        return sharedSecretKey;
    }

    @ConfigDescription("256 bit, base64-encoded secret key used to secure segment identifiers")
    @Config("protocol.spooling.shared-secret-key")
    @ConfigSecuritySensitive
    public SpoolingConfig setSharedSecretKey(String sharedEncryptionKey)
    {
        this.sharedSecretKey = sharedEncryptionKey != null ? new SecretKeySpec(getDecoder().decode(sharedEncryptionKey), "AES") : null;
        return this;
    }

    public SegmentRetrievalMode getRetrievalMode()
    {
        return retrievalMode;
    }

    @Config("protocol.spooling.retrieval-mode")
    @ConfigDescription("Determines how the client will retrieve the segment")
    public SpoolingConfig setRetrievalMode(SegmentRetrievalMode retrievalMode)
    {
        this.retrievalMode = retrievalMode;
        return this;
    }

    @MinDataSize("1kB")
    @MaxDataSize("128MB")
    public DataSize getInitialSegmentSize()
    {
        return initialSegmentSize;
    }

    @Config("protocol.spooling.initial-segment-size")
    @ConfigDescription("Initial size of the spooled segments in bytes")
    public SpoolingConfig setInitialSegmentSize(DataSize initialSegmentSize)
    {
        this.initialSegmentSize = initialSegmentSize;
        return this;
    }

    @MinDataSize("1kB")
    @MaxDataSize("128MB")
    public DataSize getMaximumSegmentSize()
    {
        return maximumSegmentSize;
    }

    @LegacyConfig("protocol.spooling.maximum-segment-size")
    @Config("protocol.spooling.max-segment-size")
    @ConfigDescription("Maximum size of the spooled segments in bytes")
    public SpoolingConfig setMaximumSegmentSize(DataSize maximumSegmentSize)
    {
        this.maximumSegmentSize = maximumSegmentSize;
        return this;
    }

    public boolean isInliningEnabled()
    {
        return inliningEnabled;
    }

    @ConfigDescription("Allow spooling protocol to inline data")
    @Config("protocol.spooling.inlining.enabled")
    public SpoolingConfig setInliningEnabled(boolean inliningEnabled)
    {
        this.inliningEnabled = inliningEnabled;
        return this;
    }

    @Min(1)
    @Max(1_000_000)
    public long getInliningMaxRows()
    {
        return inliningMaxRows;
    }

    @Config("protocol.spooling.inlining.max-rows")
    @ConfigDescription("Maximum number of rows that are allowed to be inlined per worker")
    public SpoolingConfig setInliningMaxRows(long inliningMaxRows)
    {
        this.inliningMaxRows = inliningMaxRows;
        return this;
    }

    @MinDataSize("1kB")
    @MaxDataSize("3MB")
    public DataSize getInliningMaxSize()
    {
        return inliningMaxSize;
    }

    @Config("protocol.spooling.inlining.max-size")
    @ConfigDescription("Maximum size of rows that are allowed to be inlined per worker")
    public SpoolingConfig setInliningMaxSize(DataSize inliningMaxSize)
    {
        this.inliningMaxSize = inliningMaxSize;
        return this;
    }

    @AssertTrue(message = "protocol.spooling.shared-secret-key must be 256 bits long")
    public boolean isSharedEncryptionKeyAes256()
    {
        return sharedSecretKey != null && is256BitSecretKeySpec(sharedSecretKey);
    }

    @AssertTrue(message = "protocol.spooling.initial-segment-size must be smaller than protocol.spooling.maximum-segment-size")
    public boolean areSegmentSizesCorrect()
    {
        return getInitialSegmentSize().compareTo(getMaximumSegmentSize()) < 0;
    }

    public enum SegmentRetrievalMode
    {
        // Client goes for the data to:
        STORAGE, // directly to the storage with the pre-signed URI (1 round trip)
        COORDINATOR_STORAGE_REDIRECT, // coordinator and gets redirected to the storage with the pre-signed URI (2 round trips)
        COORDINATOR_PROXY, // coordinator and gets segment data through it (1 round trip)
        WORKER_PROXY, // coordinator and gets redirected to one of the available workers and gets data through it (2 round trips)
    }
}
