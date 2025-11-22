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
package io.trino.plugin.iceberg.catalog.rest;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.util.Map;

import static org.apache.iceberg.gcp.auth.GoogleAuthManager.GCP_CREDENTIALS_PATH_PROPERTY;
import static org.apache.iceberg.rest.auth.AuthProperties.AUTH_TYPE;
import static org.apache.iceberg.rest.auth.AuthProperties.AUTH_TYPE_GOOGLE;

public class GoogleAuthProperties
        implements SecurityProperties
{
    private final Map<String, String> properties;

    @Inject
    public GoogleAuthProperties(GoogleSecurityConfig config)
    {
        properties = ImmutableMap.<String, String>builder()
                .put(AUTH_TYPE, AUTH_TYPE_GOOGLE)
                .put(GCP_CREDENTIALS_PATH_PROPERTY, config.getJsonKeyFilePath())
                .put("header.x-goog-user-project", config.getProjectId())
                .buildOrThrow();
    }

    @Override
    public Map<String, String> get()
    {
        return properties;
    }
}
