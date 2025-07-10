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
package io.trino.metadata;

import io.trino.connector.CatalogServiceProvider;
import io.trino.spi.session.PropertyMetadata;

import java.util.Map;

import static io.trino.spi.StandardErrorCode.INVALID_BRANCH_PROPERTY;

public class BranchPropertyManager
        extends AbstractCatalogPropertyManager
{
    public BranchPropertyManager(CatalogServiceProvider<Map<String, PropertyMetadata<?>>> connectorProperties)
    {
        super("branch", INVALID_BRANCH_PROPERTY, connectorProperties);
    }
}
