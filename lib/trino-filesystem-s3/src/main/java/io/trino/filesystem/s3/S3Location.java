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

import io.trino.filesystem.Location;

import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

record S3Location(Location location)
{
    S3Location
    {
        requireNonNull(location, "location is null");
        checkArgument(location.scheme().isPresent(), "No scheme for S3 location: %s", location);
        checkArgument(Set.of("s3", "s3a", "s3n").contains(location.scheme().get()), "Wrong scheme for S3 location: %s", location);
        checkArgument(location.host().isPresent(), "No bucket for S3 location: %s", location);
        checkArgument(location.userInfo().isEmpty(), "S3 location contains user info: %s", location);
        checkArgument(location.port().isEmpty(), "S3 location contains port: %s", location);
    }

    public String scheme()
    {
        return location.scheme().orElseThrow();
    }

    public String bucket()
    {
        return location.host().orElseThrow();
    }

    public String key()
    {
        return location.path();
    }

    @Override
    public String toString()
    {
        return location.toString();
    }
}
