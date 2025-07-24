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
package io.trino.iam.aws;

import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Suppliers.memoize;
import static com.google.common.base.Suppliers.memoizeWithExpiration;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class IAMSecurityMappingProvider<T extends IAMSecurityMappings<E>, E extends IAMSecurityMapping, C extends IAMSecurityMappingConfig>
{
    protected final Supplier<T> mappingsProvider;
    private final Optional<String> roleCredentialName;
    private final Optional<String> colonReplacement;

    @Inject
    public IAMSecurityMappingProvider(C config, Supplier<T> mappingsProvider)
    {
        this(mappingsProvider(mappingsProvider, config.getRefreshPeriod()),
                config.getRoleCredentialName(),
                config.getColonReplacement());
    }

    public IAMSecurityMappingProvider(
            Supplier<T> mappingsProvider,
            Optional<String> roleCredentialName,
            Optional<String> colonReplacement)
    {
        this.mappingsProvider = requireNonNull(mappingsProvider, "mappingsProvider is null");
        this.roleCredentialName = requireNonNull(roleCredentialName, "roleCredentialName is null");
        this.colonReplacement = requireNonNull(colonReplacement, "colonReplacement is null");
    }

    public Optional<IAMSecurityMappingResult> getMapping(ConnectorIdentity identity)
    {
        IAMSecurityMapping mapping = mappingsProvider.get().getMapping(identity)
                .orElseThrow(() -> new AccessDeniedException("No matching IAM security mapping"));

        if (mapping.useClusterDefault()) {
            return Optional.empty();
        }

        return Optional.of(new IAMSecurityMappingResult(
                mapping.credentials(),
                selectRole(mapping, identity),
                mapping.roleSessionName().map(name -> name.replace("${USER}", identity.getUser())),
                mapping.endpoint(),
                mapping.region()));
    }

    protected Optional<String> selectRole(IAMSecurityMapping mapping, ConnectorIdentity identity)
    {
        Optional<String> optionalSelected = getRoleFromExtraCredential(identity);

        if (optionalSelected.isEmpty()) {
            if (!mapping.allowedIamRoles().isEmpty() && mapping.iamRole().isEmpty()) {
                throw new AccessDeniedException("No IAM role selected and mapping has no default role");
            }
            verify(mapping.iamRole().isPresent() || mapping.credentials().isPresent(), "mapping must have role or credential");
            return mapping.iamRole();
        }

        String selected = optionalSelected.get();

        // selected role must match default or be allowed
        if (!selected.equals(mapping.iamRole().orElse(null)) &&
                !mapping.allowedIamRoles().contains(selected)) {
            throw new AccessDeniedException("Selected IAM role is not allowed: " + selected);
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

    private static <T> Supplier<T> mappingsProvider(Supplier<T> supplier, Optional<Duration> refreshPeriod)
    {
        return refreshPeriod
                .map(refresh -> memoizeWithExpiration(supplier::get, refresh.toMillis(), MILLISECONDS))
                .orElseGet(() -> memoize(supplier::get));
    }
}
