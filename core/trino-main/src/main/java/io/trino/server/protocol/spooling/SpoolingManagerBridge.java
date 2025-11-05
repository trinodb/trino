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
import io.trino.server.protocol.spooling.SpoolingConfig.SegmentRetrievalMode;
import io.trino.spi.TrinoException;
import io.trino.spi.spool.SpooledLocation;
import io.trino.spi.spool.SpooledSegmentHandle;
import io.trino.spi.spool.SpoolingContext;
import io.trino.spi.spool.SpoolingManager;
import jakarta.ws.rs.ServiceUnavailableException;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static io.trino.spi.spool.SpooledLocation.CoordinatorLocation;
import static io.trino.spi.spool.SpooledLocation.DirectLocation;
import static io.trino.spi.spool.SpooledLocation.coordinatorLocation;
import static java.util.Base64.getUrlDecoder;
import static java.util.Base64.getUrlEncoder;
import static java.util.Objects.requireNonNull;
import static javax.crypto.Cipher.DECRYPT_MODE;
import static javax.crypto.Cipher.ENCRYPT_MODE;

public class SpoolingManagerBridge
        implements SpoolingManager
{
    private final SpoolingManagerRegistry registry;
    private final SecretKey secretKey;
    private final SegmentRetrievalMode retrievalMode;

    @Inject
    public SpoolingManagerBridge(SpoolingConfig spoolingConfig, SpoolingManagerRegistry registry)
    {
        this.registry = requireNonNull(registry, "registry is null");
        requireNonNull(spoolingConfig, "spoolingConfig is null");
        this.retrievalMode = spoolingConfig.getRetrievalMode();
        this.secretKey = spoolingConfig.getSharedSecretKey();
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
            throws IOException
    {
        return switch (retrievalMode) {
            case STORAGE -> toUri(secretKey, directLocation(handle)
                    .orElseThrow(() -> new ServiceUnavailableException("Retrieval mode is DIRECT but cannot generate pre-signed URI")));
            case COORDINATOR_STORAGE_REDIRECT, WORKER_PROXY, COORDINATOR_PROXY -> switch (delegate().location(handle)) {
                case DirectLocation _ -> throw new IllegalStateException("Expected coordinator location but got direct one");
                case CoordinatorLocation coordinatorLocation ->
                        coordinatorLocation(toUri(secretKey, coordinatorLocation.identifier()), coordinatorLocation.headers());
            };
        };
    }

    @Override
    public Optional<DirectLocation> directLocation(SpooledSegmentHandle handle)
            throws IOException
    {
        return switch (retrievalMode) {
            case STORAGE, COORDINATOR_STORAGE_REDIRECT -> delegate().directLocation(handle);
            case COORDINATOR_PROXY, WORKER_PROXY -> throw new TrinoException(CONFIGURATION_INVALID, "Retrieval mode doesn't allow for direct storage access");
        };
    }

    @Override
    public SpooledSegmentHandle handle(Slice identifier, Map<String, List<String>> headers)
    {
        return delegate().handle(fromUri(secretKey, identifier), headers);
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

    private static DirectLocation toUri(SecretKey secretKey, DirectLocation location)
    {
        return new DirectLocation(toUri(secretKey, location.identifier()), location.directUri(), location.headers());
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
