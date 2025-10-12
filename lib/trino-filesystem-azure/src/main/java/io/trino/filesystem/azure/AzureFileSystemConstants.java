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
package io.trino.filesystem.azure;

public final class AzureFileSystemConstants
{
    /**
     * Internal property enabling {@link AzureVendedAuth} on the filesystem when set to true.
     */
    public static final String EXTRA_USE_VENDED_TOKEN = "internal$use_vended_token";

    /**
     * Internal prefix for SAS token property keys, mapping storage accounts to their SAS tokens.
     */
    public static final String EXTRA_SAS_TOKEN_PROPERTY_PREFIX = "internal$account_sas$";

    private AzureFileSystemConstants() {}
}
