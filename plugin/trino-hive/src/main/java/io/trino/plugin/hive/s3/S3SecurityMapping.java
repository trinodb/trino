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

import com.amazonaws.auth.BasicAWSCredentials;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.security.ConnectorIdentity;

import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.hive.s3.TrinoS3FileSystem.extractBucketName;
import static java.util.Objects.requireNonNull;

public class S3SecurityMapping
{
    private final Predicate<String> user;
    private final Predicate<Collection<String>> group;
    private final Predicate<URI> prefix;
    private final Optional<String> iamRole;
    private final Set<String> allowedIamRoles;
    private final Optional<String> kmsKeyId;
    private final Set<String> allowedKmsKeyIds;
    private final Optional<BasicAWSCredentials> credentials;
    private final boolean useClusterDefault;
    private final Optional<String> endpoint;
    private final Optional<String> roleSessionName;

    @JsonCreator
    public S3SecurityMapping(
            @JsonProperty("user") Optional<Pattern> user,
            @JsonProperty("group") Optional<Pattern> group,
            @JsonProperty("prefix") Optional<URI> prefix,
            @JsonProperty("iamRole") Optional<String> iamRole,
            @JsonProperty("roleSessionName") Optional<String> roleSessionName,
            @JsonProperty("allowedIamRoles") Optional<List<String>> allowedIamRoles,
            @JsonProperty("kmsKeyId") Optional<String> kmsKeyId,
            @JsonProperty("allowedKmsKeyIds") Optional<List<String>> allowedKmsKeyIds,
            @JsonProperty("accessKey") Optional<String> accessKey,
            @JsonProperty("secretKey") Optional<String> secretKey,
            @JsonProperty("useClusterDefault") Optional<Boolean> useClusterDefault,
            @JsonProperty("endpoint") Optional<String> endpoint)
    {
        this.user = requireNonNull(user, "user is null")
                .map(S3SecurityMapping::toPredicate)
                .orElse(x -> true);
        this.group = requireNonNull(group, "group is null")
                .map(S3SecurityMapping::toPredicate)
                .map(S3SecurityMapping::anyMatch)
                .orElse(x -> true);
        this.prefix = requireNonNull(prefix, "prefix is null")
                .map(S3SecurityMapping::prefixPredicate)
                .orElse(x -> true);

        this.iamRole = requireNonNull(iamRole, "iamRole is null");
        this.roleSessionName = requireNonNull(roleSessionName, "roleSessionName is null");
        checkArgument(!(iamRole.isEmpty() && roleSessionName.isPresent()), "iamRole must be provided when roleSessionName is provided");

        this.allowedIamRoles = ImmutableSet.copyOf(requireNonNull(allowedIamRoles, "allowedIamRoles is null")
                .orElse(ImmutableList.of()));

        this.kmsKeyId = requireNonNull(kmsKeyId, "kmsKeyId is null");

        this.allowedKmsKeyIds = ImmutableSet.copyOf(
                requireNonNull(allowedKmsKeyIds, "allowedKmsKeyIds is null").orElse(ImmutableList.of()));

        requireNonNull(accessKey, "accessKey is null");
        requireNonNull(secretKey, "secretKey is null");
        checkArgument(accessKey.isPresent() == secretKey.isPresent(), "accessKey and secretKey must be provided together");
        this.credentials = accessKey.map(access -> new BasicAWSCredentials(access, secretKey.get()));

        this.useClusterDefault = requireNonNull(useClusterDefault, "useClusterDefault is null")
                .orElse(false);
        boolean roleOrCredentialsArePresent = !this.allowedIamRoles.isEmpty() || iamRole.isPresent() || credentials.isPresent();
        checkArgument(this.useClusterDefault ^ roleOrCredentialsArePresent, "must either allow useClusterDefault role or provide role and/or credentials");

        checkArgument(!(this.useClusterDefault && this.kmsKeyId.isPresent()), "KMS key ID cannot be provided together with useClusterDefault");

        this.endpoint = requireNonNull(endpoint, "endpoint is null");
    }

    public boolean matches(ConnectorIdentity identity, URI uri)
    {
        return user.test(identity.getUser())
                && group.test(identity.getGroups())
                && prefix.test(uri);
    }

    public Optional<String> getIamRole()
    {
        return iamRole;
    }

    public Set<String> getAllowedIamRoles()
    {
        return allowedIamRoles;
    }

    public Optional<String> getKmsKeyId()
    {
        return kmsKeyId;
    }

    public Set<String> getAllowedKmsKeyIds()
    {
        return allowedKmsKeyIds;
    }

    public Optional<BasicAWSCredentials> getCredentials()
    {
        return credentials;
    }

    public boolean isUseClusterDefault()
    {
        return useClusterDefault;
    }

    public Optional<String> getEndpoint()
    {
        return endpoint;
    }

    public Optional<String> getRoleSessionName()
    {
        return roleSessionName;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("user", user)
                .add("group", group)
                .add("prefix", prefix)
                .add("iamRole", iamRole)
                .add("roleSessionName", roleSessionName.orElse(null))
                .add("allowedIamRoles", allowedIamRoles)
                .add("kmsKeyId", kmsKeyId)
                .add("allowedKmsKeyIds", allowedKmsKeyIds)
                .add("credentials", credentials)
                .add("useClusterDefault", useClusterDefault)
                .add("endpoint", endpoint.orElse(null))
                .toString();
    }

    private static Predicate<URI> prefixPredicate(URI prefix)
    {
        checkArgument("s3".equals(prefix.getScheme()), "prefix URI scheme is not 's3': %s", prefix);
        checkArgument(prefix.getQuery() == null, "prefix URI must not contain query: %s", prefix);
        checkArgument(prefix.getFragment() == null, "prefix URI must not contain fragment: %s", prefix);
        return value -> extractBucketName(prefix).equals(extractBucketName(value)) &&
                value.getPath().startsWith(prefix.getPath());
    }

    private static Predicate<String> toPredicate(Pattern pattern)
    {
        return value -> pattern.matcher(value).matches();
    }

    private static <T> Predicate<Collection<T>> anyMatch(Predicate<T> predicate)
    {
        return values -> values.stream().anyMatch(predicate);
    }
}
