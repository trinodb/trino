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
import io.airlift.units.DataSize;
import io.trino.util.Ciphers;
import jakarta.validation.constraints.AssertTrue;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import java.util.Optional;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Base64.getDecoder;

public class SpoolingConfig
{
    private boolean useWorkers;

    // This is implemented by the S3 and GCS which is the most common use case
    private boolean directStorageAccess = true;
    private boolean directStorageFallback;

    private boolean inlineSegments = true;

    private DataSize initialSegmentSize = DataSize.of(8, MEGABYTE);
    private DataSize maximumSegmentSize = DataSize.of(16, MEGABYTE);

    private Optional<SecretKey> sharedEncryptionKey = Optional.empty();

    public boolean isUseWorkers()
    {
        return useWorkers;
    }

    @Config("protocol.spooling.worker-access")
    @ConfigDescription("Use worker nodes to retrieve data from spooling location")
    public SpoolingConfig setUseWorkers(boolean useWorkers)
    {
        this.useWorkers = useWorkers;
        return this;
    }

    public boolean isDirectStorageAccess()
    {
        return directStorageAccess;
    }

    @Config("protocol.spooling.direct-storage-access")
    @ConfigDescription("Retrieve segments directly from the spooling location")
    public SpoolingConfig setDirectStorageAccess(boolean directStorageAccess)
    {
        this.directStorageAccess = directStorageAccess;
        return this;
    }

    public boolean isDirectStorageFallback()
    {
        return directStorageFallback;
    }

    @Config("protocol.spooling.direct-storage-fallback")
    @ConfigDescription("Fallback segment retrieval through the coordinator when direct storage access is not possible")
    public SpoolingConfig setDirectStorageFallback(boolean directStorageFallback)
    {
        this.directStorageFallback = directStorageFallback;
        return this;
    }

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

    public DataSize getMaximumSegmentSize()
    {
        return maximumSegmentSize;
    }

    @Config("protocol.spooling.maximum-segment-size")
    @ConfigDescription("Maximum size of the spooled segments in bytes")
    public SpoolingConfig setMaximumSegmentSize(DataSize maximumSegmentSize)
    {
        this.maximumSegmentSize = maximumSegmentSize;
        return this;
    }

    public boolean isInlineSegments()
    {
        return inlineSegments;
    }

    @ConfigDescription("Allow protocol to inline data")
    @Config("protocol.spooling.inline-segments")
    public SpoolingConfig setInlineSegments(boolean inlineSegments)
    {
        this.inlineSegments = inlineSegments;
        return this;
    }

    public Optional<SecretKey> getSharedEncryptionKey()
    {
        return sharedEncryptionKey;
    }

    @ConfigDescription("256 bit, base64-encoded secret key used to secure segment identifiers")
    @Config("protocol.spooling.shared-secret-key")
    @ConfigSecuritySensitive
    public SpoolingConfig setSharedEncryptionKey(String sharedEncryptionKey)
    {
        this.sharedEncryptionKey = Optional.ofNullable(sharedEncryptionKey)
                .map(value -> new SecretKeySpec(getDecoder().decode(value), "AES"));
        return this;
    }

    @AssertTrue(message = "protocol.spooling.shared-secret-key must be 256 bits long")
    public boolean isSharedEncryptionKeyAes256()
    {
        return sharedEncryptionKey
                .map(Ciphers::is256BitSecretKeySpec)
                .orElse(true);
    }

    @AssertTrue(message = "protocol.spooling.shared-secret-key must be set")
    public boolean isSharedEncryptionKeySet()
    {
        return sharedEncryptionKey.isPresent();
    }
}
