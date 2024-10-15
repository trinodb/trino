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

import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.spi.TrinoException;
import io.trino.spi.protocol.SpooledLocation;
import io.trino.spi.protocol.SpooledSegmentHandle;
import io.trino.spi.protocol.SpoolingContext;
import io.trino.spi.protocol.SpoolingManager;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static io.trino.spi.protocol.SpooledLocation.CoordinatorLocation;
import static io.trino.spi.protocol.SpooledLocation.DirectLocation;
import static io.trino.spi.protocol.SpooledLocation.coordinatorLocation;
import static java.util.Base64.getUrlDecoder;
import static java.util.Base64.getUrlEncoder;
import static java.util.Objects.requireNonNull;
import static javax.crypto.Cipher.DECRYPT_MODE;
import static javax.crypto.Cipher.ENCRYPT_MODE;

public class SpoolingManagerBridge
        implements SpoolingManager
{
    private final SpoolingManagerRegistry registry;
    private final DataSize initialSegmentSize;
    private final DataSize maximumSegmentSize;
    private final boolean inlineSegments;
    private final SecretKey secretKey;
    private final boolean directStorageAccess;
    private final boolean directStorageFallback;

    @Inject
    public SpoolingManagerBridge(SpoolingConfig spoolingConfig, SpoolingManagerRegistry registry)
    {
        this.registry = requireNonNull(registry, "registry is null");
        requireNonNull(spoolingConfig, "spoolingConfig is null");
        this.initialSegmentSize = spoolingConfig.getInitialSegmentSize();
        this.maximumSegmentSize = spoolingConfig.getMaximumSegmentSize();
        this.inlineSegments = spoolingConfig.isInlineSegments();
        this.directStorageAccess = spoolingConfig.isDirectStorageAccess();
        this.directStorageFallback = spoolingConfig.isDirectStorageFallback();
        this.secretKey = spoolingConfig.getSharedEncryptionKey()
                .orElseThrow(() -> new IllegalArgumentException("protocol.spooling.shared-secret-key is not set"));
    }

    @Override
    public long maximumSegmentSize()
    {
        return maximumSegmentSize.toBytes();
    }

    @Override
    public long initialSegmentSize()
    {
        return initialSegmentSize.toBytes();
    }

    @Override
    public boolean allowSegmentInlining()
    {
        return inlineSegments && delegate().allowSegmentInlining();
    }

    @Override
    public SpooledSegmentHandle create(SpoolingContext context)
    {
        return delegate().create(context);
    }

    @Override
    public OutputStream createOutputStream(SpooledSegmentHandle handle)
            throws IOException
    {
        return delegate().createOutputStream(handle);
    }

    @Override
    public InputStream openInputStream(SpooledSegmentHandle handle)
            throws IOException
    {
        return delegate().openInputStream(handle);
    }

    @Override
    public void acknowledge(SpooledSegmentHandle handle)
            throws IOException
    {
        delegate().acknowledge(handle);
    }

    @Override
    public SpooledLocation location(SpooledSegmentHandle handle)
    {
        return switch (delegate().location(handle)) {
            case DirectLocation directLocation -> directLocation;
            case CoordinatorLocation coordinatorLocation ->
                    coordinatorLocation(toUri(secretKey, coordinatorLocation.identifier()), coordinatorLocation.headers());
        };
    }

    @Override
    public Optional<DirectLocation> directLocation(SpooledSegmentHandle handle)
            throws IOException
    {
        if (!directStorageAccess) {
            // Disabled - client fetches data through the coordinator
            return Optional.empty();
        }

        try {
            return delegate().directLocation(handle);
        }
        catch (UnsupportedOperationException e) {
            throw new TrinoException(CONFIGURATION_INVALID, "Direct storage access is enabled but not supported by " + delegate().getClass().getSimpleName(), e);
        }
        catch (IOException e) {
            if (directStorageFallback) {
                return Optional.empty();
            }
            throw e;
        }
    }

    @Override
    public SpooledSegmentHandle handle(SpooledLocation location)
    {
        switch (location) {
            case DirectLocation _ -> throw new IllegalArgumentException("Cannot convert direct location to handle");
            case CoordinatorLocation coordinatorLocation -> {
                return delegate()
                        .handle(coordinatorLocation(fromUri(secretKey, coordinatorLocation.identifier()), coordinatorLocation.headers()));
            }
        }
    }

    private SpoolingManager delegate()
    {
        return registry
                .getSpoolingManager()
                .orElseThrow(() -> new IllegalStateException("Spooling manager is not loaded"));
    }

    private static Slice toUri(SecretKey secretKey, Slice input)
    {
        try {
            Cipher cipher = Cipher.getInstance("AES");
            cipher.init(ENCRYPT_MODE, secretKey);
            return utf8Slice(getUrlEncoder().encodeToString(cipher.doFinal(input.getBytes())));
        }
        catch (GeneralSecurityException e) {
            throw new RuntimeException("Could not encode segment identifier to URI", e);
        }
    }

    private static Slice fromUri(SecretKey secretKey, Slice input)
    {
        try {
            Cipher cipher = Cipher.getInstance("AES");
            cipher.init(DECRYPT_MODE, secretKey);
            return wrappedBuffer(cipher.doFinal(getUrlDecoder().decode(input.getBytes())));
        }
        catch (GeneralSecurityException e) {
            throw new RuntimeException("Could not decode segment identifier from URI", e);
        }
    }
}
