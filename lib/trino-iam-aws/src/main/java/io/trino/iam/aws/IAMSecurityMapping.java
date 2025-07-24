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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.BindingAnnotation;
import io.trino.spi.security.ConnectorIdentity;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

@JsonIgnoreProperties(ignoreUnknown = true)
public class IAMSecurityMapping
{
    private final Predicate<String> user;
    private final Predicate<Collection<String>> group;
    private final Optional<String> iamRole;
    private final Optional<String> roleSessionName;
    private final Set<String> allowedIamRoles;
    private final Optional<AwsCredentials> credentials;
    protected final boolean useClusterDefault;
    private final Optional<String> endpoint;
    private final Optional<String> region;
    private final Boolean requireIdentity = true;

    public IAMSecurityMapping(
            @JsonProperty("user") Optional<Pattern> user,
            @JsonProperty("group") Optional<Pattern> group,
            @JsonProperty("iamRole") Optional<String> iamRole,
            @JsonProperty("roleSessionName") Optional<String> roleSessionName,
            @JsonProperty("allowedIamRoles") Optional<List<String>> allowedIamRoles,
            @JsonProperty("accessKey") Optional<String> accessKey,
            @JsonProperty("secretKey") Optional<String> secretKey,
            @JsonProperty("useClusterDefault") Optional<Boolean> useClusterDefault,
            @JsonProperty("endpoint") Optional<String> endpoint,
            @JsonProperty("region") Optional<String> region)
    {
        this.user = user
                .map(IAMSecurityMapping::toPredicate)
                .orElse(_ -> true);
        this.group = group
                .map(IAMSecurityMapping::toPredicate)
                .map(IAMSecurityMapping::anyMatch)
                .orElse(_ -> true);

        this.iamRole = requireNonNull(iamRole, "iamRole is null");
        this.roleSessionName = requireNonNull(roleSessionName, "roleSessionName is null");
        checkArgument(roleSessionName.isEmpty() || iamRole.isPresent(), "iamRole must be provided when roleSessionName is provided");

        this.allowedIamRoles = ImmutableSet.copyOf(allowedIamRoles.orElse(ImmutableList.of()));

        requireNonNull(accessKey, "accessKey is null");
        requireNonNull(secretKey, "secretKey is null");
        checkArgument(accessKey.isPresent() == secretKey.isPresent(), "accessKey and secretKey must be provided together");
        this.credentials = accessKey.map(access -> AwsBasicCredentials.create(access, secretKey.get()));

        this.useClusterDefault = useClusterDefault.orElse(false);
        boolean roleOrCredentialsArePresent = !this.allowedIamRoles.isEmpty() || iamRole.isPresent() || credentials.isPresent();
        checkArgument(this.useClusterDefault != roleOrCredentialsArePresent, "must either allow useClusterDefault role or provide role and/or credentials");

        this.endpoint = requireNonNull(endpoint, "endpoint is null");
        this.region = requireNonNull(region, "region is null");
    }

    protected boolean matches(ConnectorIdentity identity)
    {
        return user.test(identity.getUser()) &&
                group.test(identity.getGroups());
    }

    public Optional<String> iamRole()
    {
        return iamRole;
    }

    public Optional<String> roleSessionName()
    {
        return roleSessionName;
    }

    public Set<String> allowedIamRoles()
    {
        return allowedIamRoles;
    }

    public Optional<AwsCredentials> credentials()
    {
        return credentials;
    }

    public boolean useClusterDefault()
    {
        return useClusterDefault;
    }

    public Optional<String> endpoint()
    {
        return endpoint;
    }

    public Optional<String> region()
    {
        return region;
    }

    private static Predicate<String> toPredicate(Pattern pattern)
    {
        return value -> pattern.matcher(value).matches();
    }

    private static <T> Predicate<Collection<T>> anyMatch(Predicate<T> predicate)
    {
        return values -> values.stream().anyMatch(predicate);
    }

    @Retention(RUNTIME)
    @Target({FIELD, PARAMETER, METHOD})
    @BindingAnnotation
    public @interface ForIAMSecurityMapping {}
}
