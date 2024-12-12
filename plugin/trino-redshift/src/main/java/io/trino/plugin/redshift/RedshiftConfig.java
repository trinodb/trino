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
package io.trino.plugin.redshift;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import jakarta.annotation.PostConstruct;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Pattern;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;

@DefunctConfig({
        "redshift.disable-automatic-fetch-size",
        "redshift.use-legacy-type-mapping",
})
public class RedshiftConfig
{
    private Integer fetchSize;

    private String unloadLocation;
    private String unloadIamRole;

    public Optional<@Min(0) Integer> getFetchSize()
    {
        return Optional.ofNullable(fetchSize);
    }

    @Config("redshift.fetch-size")
    @ConfigDescription("Redshift fetch size, trino specific heuristic is applied if empty")
    public RedshiftConfig setFetchSize(Integer fetchSize)
    {
        this.fetchSize = fetchSize;
        return this;
    }

    public Optional<@Pattern(regexp = "^s3://[a-z0-9][a-z0-9.-]*(?:/[a-z0-9._/-]+)?$", message = "Invalid S3 unload path") String> getUnloadLocation()
    {
        return Optional.ofNullable(unloadLocation);
    }

    @Config("redshift.unload-location")
    @ConfigDescription("A writeable location in Amazon S3, to be used for unloading Redshift query results")
    public RedshiftConfig setUnloadLocation(String unloadLocation)
    {
        this.unloadLocation = unloadLocation;
        return this;
    }

    public Optional<String> getUnloadIamRole()
    {
        return Optional.ofNullable(unloadIamRole);
    }

    @Config("redshift.unload-iam-role")
    @ConfigDescription("Fully specified ARN of the IAM Role attached to the Redshift cluster")
    public RedshiftConfig setUnloadIamRole(String unloadIamRole)
    {
        this.unloadIamRole = unloadIamRole;
        return this;
    }

    @PostConstruct
    public void validate()
    {
        checkState(getUnloadIamRole().isPresent() == getUnloadLocation().isPresent(),
                "Either 'redshift.unload-iam-role' and 'redshift.unload-location' must be set or both of them must not be set");
    }
}
