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
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Pattern;

import java.util.Optional;

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

    public Optional<@Pattern(regexp = "^s3://[^/]+(/[^/]+)?$", message = "Path shouldn't end with trailing slash") String> getUnloadLocation()
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
    @ConfigDescription("Fully specified ARN of the IAM Role attached to the Redshift cluster and having access to S3")
    public RedshiftConfig setUnloadIamRole(String unloadIamRole)
    {
        this.unloadIamRole = unloadIamRole;
        return this;
    }
}
