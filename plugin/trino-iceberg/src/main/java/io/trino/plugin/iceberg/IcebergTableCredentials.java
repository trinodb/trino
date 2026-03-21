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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ConnectorTableCredentials;
import org.apache.iceberg.io.FileIO;

import java.util.Map;

public record IcebergTableCredentials(Map<String, String> fileIoProperties, long credentialsFetchedAtMs)
        implements ConnectorTableCredentials
{
    // The presence of this key in fileIoProperties signals that the REST catalog vended
    // short-lived S3 credentials that must be periodically refreshed.
    private static final String VENDED_S3_ACCESS_KEY = "s3.access-key-id";

    public IcebergTableCredentials
    {
        fileIoProperties = ImmutableMap.copyOf(fileIoProperties);
    }

    public static IcebergTableCredentials forFileIO(FileIO io)
    {
        Map<String, String> properties = io.properties();
        // Set credentialsFetchedAtMs only when actual vended S3 credentials are present.
        // A zero timestamp disables the worker-side refresh path (see IcebergUtil.maybeRefreshVendedCredentials),
        // so catalogs that do not vend credentials are unaffected even if getTableCredentials() is called.
        long fetchedAt = properties.containsKey(VENDED_S3_ACCESS_KEY) ? System.currentTimeMillis() : 0L;
        return new IcebergTableCredentials(properties, fetchedAt);
    }
}
