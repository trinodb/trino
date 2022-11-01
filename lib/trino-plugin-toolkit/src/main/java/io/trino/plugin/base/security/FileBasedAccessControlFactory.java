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

package io.trino.plugin.base.security;

import io.trino.plugin.base.CatalogName;

import java.io.File;
import java.util.function.Supplier;

import static io.trino.plugin.base.util.JsonUtils.parseJson;

public final class FileBasedAccessControlFactory
{
    private FileBasedAccessControlFactory()
    {
    }

    public static JsonAccessControl create(CatalogName catalogName, File configFile)
    {
        Supplier<AccessControlRules> fileBasedRulesProvider = () ->
                parseJson(configFile.toPath(), AccessControlRules.class);
        return new JsonAccessControl(catalogName, fileBasedRulesProvider);
    }
}
