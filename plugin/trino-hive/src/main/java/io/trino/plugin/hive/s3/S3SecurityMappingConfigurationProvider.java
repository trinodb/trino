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
package io.trino.plugin.hive.s3;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import io.airlift.log.Logger;
import io.trino.plugin.hive.DynamicConfigurationProvider;
import io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import io.trino.spi.security.AccessDeniedException;
import org.apache.hadoop.conf.Configuration;

import javax.inject.Inject;

import java.net.URI;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.hive.DynamicConfigurationProvider.setCacheKey;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_ACCESS_KEY;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_ENDPOINT;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_IAM_ROLE;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_KMS_KEY_ID;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.S3_SECRET_KEY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class S3SecurityMappingConfigurationProvider
        implements DynamicConfigurationProvider
{
    private static final Logger log = Logger.get(S3SecurityMappingConfigurationProvider.class);

    private static final Set<String> SCHEMES = ImmutableSet.of("s3", "s3a", "s3n");
    private static final String ANY_KMS_KEY_ID = "*";

    private final Supplier<S3SecurityMappings> mappings;
    private final Optional<String> roleCredentialName;
    private final Optional<String> kmsKeyIdCredentialName;
    private final Optional<String> colonReplacement;

    @Inject
    public S3SecurityMappingConfigurationProvider(S3SecurityMappingConfig config, S3SecurityMappingsProvider mappingsProvider)
    {
        this(getMappings(config, mappingsProvider), config.getRoleCredentialName(), config.getKmsKeyIdCredentialName(), config.getColonReplacement());
    }

    public S3SecurityMappingConfigurationProvider(Supplier<S3SecurityMappings> mappings, Optional<String> roleCredentialName, Optional<String> kmsKeyIdCredentialName, Optional<String> colonReplacement)
    {
        this.mappings = requireNonNull(mappings, "mappings is null");
        this.roleCredentialName = requireNonNull(roleCredentialName, "roleCredentialName is null");
        this.kmsKeyIdCredentialName = requireNonNull(kmsKeyIdCredentialName, "kmsKeyIdCredentialName is null");
        this.colonReplacement = requireNonNull(colonReplacement, "colonReplacement is null");
    }

    private static Supplier<S3SecurityMappings> getMappings(S3SecurityMappingConfig config, S3SecurityMappingsProvider supplier)
    {
        String configFilePath = config.getConfigFilePath().orElseThrow(() -> new IllegalArgumentException("config file not set"));
        if (config.getRefreshPeriod().isEmpty()) {
            return Suppliers.memoize(supplier::get);
        }
        return Suppliers.memoizeWithExpiration(
                () -> {
                    log.info("Refreshing S3 security mapping configuration from %s", configFilePath);
                    return supplier.get();
                },
                config.getRefreshPeriod().get().toMillis(),
                MILLISECONDS);
    }

    @Override
    public void updateConfiguration(Configuration configuration, HdfsContext context, URI uri)
    {
        if (!SCHEMES.contains(uri.getScheme())) {
            return;
        }

        S3SecurityMapping mapping = mappings.get().getMapping(context.getIdentity(), uri)
                .orElseThrow(() -> new AccessDeniedException("No matching S3 security mapping"));
        if (mapping.isUseClusterDefault()) {
            return;
        }

        Hasher hasher = Hashing.sha256().newHasher();

        mapping.getCredentials().ifPresent(credentials -> {
            configuration.set(S3_ACCESS_KEY, credentials.getAWSAccessKeyId());
            configuration.set(S3_SECRET_KEY, credentials.getAWSSecretKey());
            hasher.putString(credentials.getAWSAccessKeyId(), UTF_8);
            hasher.putString(credentials.getAWSSecretKey(), UTF_8);
        });

        selectRole(mapping, context).ifPresent(role -> {
            configuration.set(S3_IAM_ROLE, role);
            hasher.putString(role, UTF_8);
        });

        selectKmsKeyId(mapping, context).ifPresent(key -> {
            configuration.set(S3_KMS_KEY_ID, key);
            hasher.putString(S3_KMS_KEY_ID + ":" + key, UTF_8);
        });

        mapping.getEndpoint().ifPresent(endpoint -> {
            configuration.set(S3_ENDPOINT, endpoint);
            hasher.putString(endpoint, UTF_8);
        });

        setCacheKey(configuration, hasher.hash().toString());
    }

    private Optional<String> selectRole(S3SecurityMapping mapping, HdfsContext context)
    {
        Optional<String> optionalSelected = getRoleFromExtraCredential(context);

        if (optionalSelected.isEmpty()) {
            if (!mapping.getAllowedIamRoles().isEmpty() && mapping.getIamRole().isEmpty()) {
                throw new AccessDeniedException("No S3 role selected and mapping has no default role");
            }
            verify(mapping.getIamRole().isPresent() || mapping.getCredentials().isPresent(), "mapping must have role or credential");
            return mapping.getIamRole();
        }

        String selected = optionalSelected.get();

        // selected role must match default or be allowed
        if (!selected.equals(mapping.getIamRole().orElse(null)) &&
                !mapping.getAllowedIamRoles().contains(selected)) {
            throw new AccessDeniedException("Selected S3 role is not allowed: " + selected);
        }

        return optionalSelected;
    }

    private Optional<String> getRoleFromExtraCredential(HdfsContext context)
    {
        Optional<String> extraCredentialRole = roleCredentialName.map(name -> context.getIdentity().getExtraCredentials().get(name));

        if (colonReplacement.isPresent()) {
            return extraCredentialRole.map(role -> role.replace(colonReplacement.get(), ":"));
        }
        return extraCredentialRole;
    }

    private Optional<String> selectKmsKeyId(S3SecurityMapping mapping, HdfsContext context)
    {
        Optional<String> userSelected = getKmsKeyIdFromExtraCredential(context);

        if (userSelected.isEmpty()) {
            return mapping.getKmsKeyId();
        }

        String selected = userSelected.get();

        // selected key id must match default or be allowed
        if (!selected.equals(mapping.getKmsKeyId().orElse(null)) &&
                !mapping.getAllowedKmsKeyIds().contains(selected) &&
                !mapping.getAllowedKmsKeyIds().contains(ANY_KMS_KEY_ID)) {
            throw new AccessDeniedException("Selected KMS Key ID is not allowed");
        }

        return userSelected;
    }

    private Optional<String> getKmsKeyIdFromExtraCredential(HdfsContext context)
    {
        return kmsKeyIdCredentialName.map(name -> context.getIdentity().getExtraCredentials().get(name));
    }
}
