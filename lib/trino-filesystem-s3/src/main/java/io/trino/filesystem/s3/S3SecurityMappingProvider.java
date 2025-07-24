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
import io.trino.filesystem.Location;
import io.trino.iam.aws.IAMSecurityMappingProvider;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Optional;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

final class S3SecurityMappingProvider
        extends IAMSecurityMappingProvider<S3SecurityMappings, S3SecurityMapping, S3SecurityMappingConfig>
{
    private final Optional<String> kmsKeyIdCredentialName;
    private final Optional<String> sseCustomerKeyCredentialName;

    @Inject
    public S3SecurityMappingProvider(S3SecurityMappingConfig config, Supplier<S3SecurityMappings> mappings)
    {
        super(config, mappings);

        this.kmsKeyIdCredentialName = requireNonNull(config.getKmsKeyIdCredentialName(), "kmsKeyIdCredentialName is null");
        this.sseCustomerKeyCredentialName = requireNonNull(config.getSseCustomerKeyCredentialName(), "sseCustomerKeyCredentialName is null");
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
                getSseCustomerKey(mapping, identity),
                mapping.endpoint(),
                mapping.region()));
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

    private Optional<String> getSseCustomerKey(S3SecurityMapping mapping, ConnectorIdentity identity)
    {
        Optional<String> providedKey = getSseCustomerKeyFromExtraCredential(identity);

        if (providedKey.isEmpty()) {
            return mapping.sseCustomerKey();
        }
        if (mapping.sseCustomerKey().isPresent() && mapping.allowedSseCustomerKeys().isEmpty()) {
            throw new AccessDeniedException("allowedSseCustomerKeys must be set if sseCustomerKey is provided");
        }

        String selected = providedKey.get();

        if (selected.equals(mapping.sseCustomerKey().orElse(null)) ||
                mapping.allowedSseCustomerKeys().contains(selected) ||
                mapping.allowedSseCustomerKeys().contains("*")) {
            return providedKey;
        }
        throw new AccessDeniedException("Provided SSE Customer Key is not allowed");
    }

    private Optional<String> getSseCustomerKeyFromExtraCredential(ConnectorIdentity identity)
    {
        return sseCustomerKeyCredentialName.map(name -> identity.getExtraCredentials().get(name));
    }
}
