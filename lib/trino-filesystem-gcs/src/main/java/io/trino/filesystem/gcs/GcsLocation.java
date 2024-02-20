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
package io.trino.filesystem.gcs;

import io.trino.filesystem.Location;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

record GcsLocation(Location location)
{
    GcsLocation
    {
        // Note: Underscores are allowed in bucket names, see https://cloud.google.com/storage/docs/buckets#naming.
        // Generation numbers are not supported, i.e. gs://bucket/path#generation.
        // For more details on generation numbers see https://cloud.google.com/storage/docs/object-versioning.

        requireNonNull(location, "location");
        checkArgument(location.scheme().isPresent(), "No scheme for GCS location: %s", location);
        checkArgument(location.scheme().get().equals("gs"), "Wrong scheme for S3 location: %s", location);
        checkArgument(location.host().isPresent(), "No bucket for GCS location: %s", location);
        checkArgument(location.userInfo().isEmpty(), "GCS location contains user info: %s", location);
        checkArgument(location.port().isEmpty(), "GCS location contains port: %s", location);
        checkArgument(!location.path().contains("#"), "GCS generation numbers are not supported: %s", location);
        checkArgument(!location.path().contains("?"), "Invalid character '?': %s", location);
    }

    public String scheme()
    {
        return location.scheme().orElseThrow();
    }

    public String bucket()
    {
        return location.host().orElseThrow();
    }

    public String path()
    {
        return location.path();
    }

    public String getBase()
    {
        return "%s://%s/".formatted(scheme(), bucket());
    }

    @Override
    public String toString()
    {
        return location.toString();
    }
}
