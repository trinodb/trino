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
import io.trino.spi.security.ConnectorIdentity;

import java.util.Optional;

public interface S3CredentialsMapper
{
    /**
     * Returns S3 credentials and configuration for the given identity and location.
     * Empty result indicates cluster defaults should be used.
     */
    Optional<S3SecurityMappingResult> getMapping(ConnectorIdentity identity, Location location);
}
