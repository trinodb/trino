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
package io.trino.filesystem.s3;

import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.filesystem.Location;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Suppliers.memoize;
import static com.google.common.base.Suppliers.memoizeWithExpiration;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

final class S3SecurityMappingProvider
{
    private final Supplier<S3SecurityMappings> mappingsProvider;
    private final Optional<String> roleCredentialName;
    private final Optional<String> kmsKeyIdCredentialName;
    private final Optional<String> colonReplacement;

    @Inject
    public S3SecurityMappingProvider(S3SecurityMappingConfig config, Supplier<S3SecurityMappings> mappingsProvider)
    {
        this(mappingsProvider(mappingsProvider, config.getRefreshPeriod()),
                config.getRoleCredentialName(),
                config.getKmsKeyIdCredentialName(),
                config.getColonReplacement());
    }

    public S3SecurityMappingProvider(
            Supplier<S3SecurityMappings> mappingsProvider,
            Optional<String> roleCredentialName,
            Optional<String> kmsKeyIdCredentialName,
            Optional<String> colonReplacement)
    {
        this.mappingsProvider = requireNonNull(mappingsProvider, "mappingsProvider is null");
        this.roleCredentialName = requireNonNull(roleCredentialName, "roleCredentialName is null");
        this.kmsKeyIdCredentialName = requireNonNull(kmsKeyIdCredentialName, "kmsKeyIdCredentialName is null");
        this.colonReplacement = requireNonNull(colonReplacement, "colonReplacement is null");
    }

    public Optional<S3SecurityMappingResult> getMapping(ConnectorIdentity identity, Location location)
    {
        S3SecurityMapping mapping = mappingsProvider.get().getMapping(identity, new S3Location(location))
                .orElseThrow(() -> new AccessDeniedException("No matching S3 security mapping"));

        if (mapping.useClusterDefault()) {
            return Optional.empty();
        }

        return Optional.of(new S3SecurityMappingResult(
                mapping.credentials(),
                selectRole(mapping, identity),
                mapping.roleSessionName().map(name -> name.replace("${USER}", identity.getUser())),
                selectKmsKeyId(mapping, identity),
                mapping.endpoint(),
                mapping.region()));
    }

    private Optional<String> selectRole(S3SecurityMapping mapping, ConnectorIdentity identity)
    {
        Optional<String> optionalSelected = getRoleFromExtraCredential(identity);

        if (optionalSelected.isEmpty()) {
            if (!mapping.allowedIamRoles().isEmpty() && mapping.iamRole().isEmpty()) {
                throw new AccessDeniedException("No S3 role selected and mapping has no default role");
            }
            verify(mapping.iamRole().isPresent() || mapping.credentials().isPresent(), "mapping must have role or credential");
            return mapping.iamRole();
        }

        String selected = optionalSelected.get();

        // selected role must match default or be allowed
        if (!selected.equals(mapping.iamRole().orElse(null)) &&
                !mapping.allowedIamRoles().contains(selected)) {
            throw new AccessDeniedException("Selected S3 role is not allowed: " + selected);
        }

        return optionalSelected;
    }

    private Optional<String> getRoleFromExtraCredential(ConnectorIdentity identity)
    {
        return roleCredentialName
                .map(name -> identity.getExtraCredentials().get(name))
                .map(role -> colonReplacement
                        .map(replacement -> role.replace(replacement, ":"))
                        .orElse(role));
    }

    private Optional<String> selectKmsKeyId(S3SecurityMapping mapping, ConnectorIdentity identity)
    {
        Optional<String> userSelected = getKmsKeyIdFromExtraCredential(identity);

        if (userSelected.isEmpty()) {
            return mapping.kmsKeyId();
        }

        String selected = userSelected.get();

        // selected key ID must match default or be allowed
        if (!selected.equals(mapping.kmsKeyId().orElse(null)) &&
                !mapping.allowedKmsKeyIds().contains(selected) &&
                !mapping.allowedKmsKeyIds().contains("*")) {
            throw new AccessDeniedException("Selected KMS Key ID is not allowed");
        }

        return userSelected;
    }

    private Optional<String> getKmsKeyIdFromExtraCredential(ConnectorIdentity identity)
    {
        return kmsKeyIdCredentialName.map(name -> identity.getExtraCredentials().get(name));
    }

    private static Supplier<S3SecurityMappings> mappingsProvider(Supplier<S3SecurityMappings> supplier, Optional<Duration> refreshPeriod)
    {
        return refreshPeriod
                .map(refresh -> memoizeWithExpiration(supplier::get, refresh.toMillis(), MILLISECONDS))
                .orElseGet(() -> memoize(supplier::get));
    }
}
