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
package io.trino.plugin.paimon;

import com.google.inject.Inject;
import io.trino.plugin.paimon.catalog.PaimonTrinoCatalogFactory;
import io.trino.spi.security.ConnectorIdentity;

/**
 * A factory to create {@link PaimonMetadata}.
 */
public class PaimonMetadataFactory
{
    private final PaimonTrinoCatalogFactory paimonTrinoCatalogFactory;

    @Inject
    public PaimonMetadataFactory(PaimonTrinoCatalogFactory paimonTrinoCatalogFactory)
    {
        this.paimonTrinoCatalogFactory = paimonTrinoCatalogFactory;
    }

    public PaimonMetadata create(ConnectorIdentity identity)
    {
        return new PaimonMetadata(paimonTrinoCatalogFactory.create(identity));
    }
}
