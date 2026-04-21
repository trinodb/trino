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
package io.trino.plugin.hive.crypto;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.parquet.crypto.DecryptionKeyRetriever;
import org.apache.parquet.hadoop.metadata.ColumnPath;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Reads keys from two environment variables.
 * <pre>
 *   pme.environment-key-retriever.footer-keys  =  &lt;single‑key&gt; | id1:key1,id2:key2 …
 *   pme.environment-key-retriever.column-keys  =  &lt;single‑key&gt; | id1:key1,id2:key2 …
 * </pre>
 * <ul>
 *   <li>If the value contains ‘:’ we treat it as a <em>map</em> (comma‑separated {@code id:key}).
 *       The {@code id} must match the {@code keyMetadata} supplied by Parquet.</li>
 *   <li>Otherwise it is a single default key, independent of {@code keyMetadata}.</li>
 *   <li>Keys are expected to be Base‑64; if decoding fails we fall back to the raw UTF‑8 bytes.</li>
 * </ul>
 */
public final class EnvironmentDecryptionKeyRetriever
        implements DecryptionKeyRetriever
{
    private static final String FOOTER_VARIABLE_NAME = "pme.environment-key-retriever.footer-keys";
    private static final String COLUMN_VARIABLE_NAME = "pme.environment-key-retriever.column-keys";

    private final KeySource footerKeys;
    private final KeySource columnKeys;

    @Inject
    public EnvironmentDecryptionKeyRetriever()
    {
        this(parseEnvironmentVariable(FOOTER_VARIABLE_NAME), parseEnvironmentVariable(COLUMN_VARIABLE_NAME));
    }

    @VisibleForTesting
    EnvironmentDecryptionKeyRetriever(String footerValue, String columnValue)
    {
        this(parseValue(footerValue, FOOTER_VARIABLE_NAME), parseValue(columnValue, COLUMN_VARIABLE_NAME));
    }

    private EnvironmentDecryptionKeyRetriever(KeySource footerKeys, KeySource columnKeys)
    {
        this.footerKeys = footerKeys;
        this.columnKeys = columnKeys;
    }

    @Override
    public Optional<byte[]> getColumnKey(ColumnPath columnPath, Optional<byte[]> keyMetadata)
    {
        return columnKeys.resolve(keyMetadata);
    }

    @Override
    public Optional<byte[]> getFooterKey(Optional<byte[]> keyMetadata)
    {
        return footerKeys.resolve(keyMetadata);
    }

    private static KeySource parseEnvironmentVariable(String variable)
    {
        return parseValue(System.getenv(variable), variable);
    }

    private static KeySource parseValue(String value, String variable)
    {
        if (value == null || value.isBlank()) {
            return KeySource.empty();
        }
        if (value.contains(":")) {
            // map mode
            ImmutableMap.Builder<ByteBuffer, byte[]> map = ImmutableMap.builder();
            for (String entry : value.split("\\s*,\\s*")) {
                checkArgument(!entry.isBlank(), "Empty entry in %s", variable);
                if (entry.isBlank()) {
                    continue;
                }
                String[] parts = entry.split(":", 2);
                checkArgument(parts.length == 2, "Malformed entry in %s: %s", variable, entry);
                map.put(ByteBuffer.wrap(parts[0].getBytes(StandardCharsets.UTF_8)), decodeKey(parts[1]));
            }
            return new KeySource(Optional.empty(), map.buildOrThrow());
        }
        // single key mode
        return new KeySource(Optional.of(decodeKey(value)), ImmutableMap.of());
    }

    private static byte[] decodeKey(String token)
    {
        return Base64.getDecoder().decode(token);
    }

    /**
     * container for either a single default key or a map keyed by key‑metadata id
     */
    private record KeySource(Optional<byte[]> singleKey, Map<ByteBuffer, byte[]> keyedKeys)
    {
        static KeySource empty()
        {
            return new KeySource(Optional.empty(), Collections.emptyMap());
        }

        Optional<byte[]> resolve(Optional<byte[]> keyMetadata)
        {
            if (singleKey.isPresent()) {
                // no map → keyMetadata irrelevant
                return singleKey;
            }
            return keyMetadata.map(bytes -> keyedKeys.get(ByteBuffer.wrap(bytes)));
        }
    }
}
