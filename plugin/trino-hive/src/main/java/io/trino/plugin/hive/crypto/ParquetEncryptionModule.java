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

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.parquet.crypto.DecryptionKeyRetriever;
import io.trino.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.hadoop.metadata.ColumnPath;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class ParquetEncryptionModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        ParquetEncryptionConfig config = buildConfigObject(ParquetEncryptionConfig.class);
        Multibinder<DecryptionKeyRetriever> retrieverBinder =
                Multibinder.newSetBinder(binder, DecryptionKeyRetriever.class);
        if (config.isEnvironmentKeyRetrieverEnabled()) {
            retrieverBinder.addBinding().to(EnvironmentDecryptionKeyRetriever.class).in(Scopes.SINGLETON);
        }
    }

    @Provides
    @Singleton
    public Optional<FileDecryptionProperties> fileDecryptionProperties(
            ParquetEncryptionConfig config,
            Set<DecryptionKeyRetriever> retrievers)
    {
        if (retrievers.isEmpty()) {
            return Optional.empty();
        }

        DecryptionKeyRetriever aggregate = new CompositeDecryptionKeyRetriever(List.copyOf(retrievers));

        FileDecryptionProperties.Builder builder = FileDecryptionProperties.builder()
                .withKeyRetriever(aggregate)
                .withCheckFooterIntegrity(config.isCheckFooterIntegrity());

        config.getAadPrefix()
                .map(string -> string.getBytes(StandardCharsets.UTF_8))
                .ifPresent(builder::withAadPrefix);

        return Optional.of(builder.build());
    }

    private static class CompositeDecryptionKeyRetriever
            implements DecryptionKeyRetriever
    {
        private final List<DecryptionKeyRetriever> delegates;

        CompositeDecryptionKeyRetriever(List<DecryptionKeyRetriever> delegates)
        {
            this.delegates = List.copyOf(delegates);
        }

        @Override
        public Optional<byte[]> getColumnKey(ColumnPath path, Optional<byte[]> meta)
        {
            return delegates.stream()
                    .map(delegate -> delegate.getColumnKey(path, meta))
                    .filter(Optional::isPresent)
                    .findFirst()
                    .orElse(Optional.empty());
        }

        @Override
        public Optional<byte[]> getFooterKey(Optional<byte[]> meta)
        {
            return delegates.stream()
                    .map(delegate -> delegate.getFooterKey(meta))
                    .filter(Optional::isPresent)
                    .findFirst()
                    .orElse(Optional.empty());
        }
    }
}
