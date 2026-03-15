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

public final class GcsFileSystemConstants
{
    public static final String EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_PROPERTY = "internal$gcs_oauth2_token";
    public static final String EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_EXPIRES_AT_PROPERTY = "internal$gcs_oauth2_token_expires_at";
    public static final String EXTRA_CREDENTIALS_GCS_PROJECT_ID_PROPERTY = "internal$gcs_project_id";
    public static final String EXTRA_CREDENTIALS_GCS_SERVICE_HOST_PROPERTY = "internal$gcs_service_host";
    public static final String EXTRA_CREDENTIALS_GCS_NO_AUTH_PROPERTY = "internal$gcs_no_auth";
    public static final String EXTRA_CREDENTIALS_GCS_USER_PROJECT_PROPERTY = "internal$gcs_user_project";

    private GcsFileSystemConstants() {}
}
