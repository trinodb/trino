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

import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableMap;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogConfig.SessionType;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.junit.jupiter.api.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTrinoRestCatalogCaseSensitivity
{
    private static final TypeManager TYPE_MANAGER = new TestingTypeManager();
    private static final CatalogName CATALOG_NAME = new CatalogName("test_catalog");
    private static final String TRINO_VERSION = "test-version";

    @Test
    public void testCaseInsensitiveCanonicalizeNonDelimited()
    {
        TrinoRestCatalog catalog = createCatalog(true); // case insensitive

        // Non-delimited identifiers should be lowercased
        assertThat(catalog.canonicalize("MixedCase", false)).isEqualTo("mixedcase");
        assertThat(catalog.canonicalize("UPPERCASE", false)).isEqualTo("uppercase");
        assertThat(catalog.canonicalize("lowercase", false)).isEqualTo("lowercase");
    }

    @Test
    public void testCaseInsensitiveCanonicalizeDelimited()
    {
        TrinoRestCatalog catalog = createCatalog(true); // case insensitive

        // Delimited identifiers should still be lowercased in case-insensitive mode
        assertThat(catalog.canonicalize("MixedCase", true)).isEqualTo("mixedcase");
        assertThat(catalog.canonicalize("UPPERCASE", true)).isEqualTo("uppercase");
        assertThat(catalog.canonicalize("lowercase", true)).isEqualTo("lowercase");
    }

    @Test
    public void testCaseSensitiveCanonicalizeNonDelimited()
    {
        TrinoRestCatalog catalog = createCatalog(false); // case sensitive

        // Non-delimited identifiers should still be lowercased
        assertThat(catalog.canonicalize("MixedCase", false)).isEqualTo("mixedcase");
        assertThat(catalog.canonicalize("UPPERCASE", false)).isEqualTo("uppercase");
        assertThat(catalog.canonicalize("lowercase", false)).isEqualTo("lowercase");
    }

    @Test
    public void testCaseSensitiveCanonicalizeDelimited()
    {
        TrinoRestCatalog catalog = createCatalog(false); // case sensitive

        // Delimited identifiers should preserve case in case-sensitive mode
        assertThat(catalog.canonicalize("MixedCase", true)).isEqualTo("MixedCase");
        assertThat(catalog.canonicalize("UPPERCASE", true)).isEqualTo("UPPERCASE");
        assertThat(catalog.canonicalize("lowercase", true)).isEqualTo("lowercase");
    }

    @Test
    public void testSpecialCharacters()
    {
        TrinoRestCatalog caseSensitive = createCatalog(false);
        TrinoRestCatalog caseInsensitive = createCatalog(true);

        String specialName = "Table_With-Special.Chars123";

        // Non-delimited should be lowercased in both modes
        assertThat(caseSensitive.canonicalize(specialName, false)).isEqualTo("table_with-special.chars123");
        assertThat(caseInsensitive.canonicalize(specialName, false)).isEqualTo("table_with-special.chars123");

        // Delimited should preserve case only in case-sensitive mode
        assertThat(caseSensitive.canonicalize(specialName, true)).isEqualTo("Table_With-Special.Chars123");
        assertThat(caseInsensitive.canonicalize(specialName, true)).isEqualTo("table_with-special.chars123");
    }

    @Test
    public void testEmptyAndNullStrings()
    {
        TrinoRestCatalog catalog = createCatalog(false);

        assertThat(catalog.canonicalize("", false)).isEqualTo("");
        assertThat(catalog.canonicalize("", true)).isEqualTo("");
    }

    @Test
    public void testUnicodeCharacters()
    {
        TrinoRestCatalog caseSensitive = createCatalog(false);
        TrinoRestCatalog caseInsensitive = createCatalog(true);

        String unicodeName = "Tábla_Ñame";

        // Non-delimited should be lowercased
        assertThat(caseSensitive.canonicalize(unicodeName, false)).isEqualTo("tábla_ñame");
        assertThat(caseInsensitive.canonicalize(unicodeName, false)).isEqualTo("tábla_ñame");

        // Delimited behavior depends on case sensitivity
        assertThat(caseSensitive.canonicalize(unicodeName, true)).isEqualTo("Tábla_Ñame");
        assertThat(caseInsensitive.canonicalize(unicodeName, true)).isEqualTo("tábla_ñame");
    }

    private static TrinoRestCatalog createCatalog(boolean caseInsensitive)
    {
        RESTSessionCatalog restSessionCatalog = new RESTSessionCatalog();
        IcebergRestCatalogConfig restCatalogConfig = new IcebergRestCatalogConfig();
        Cache<Namespace, Namespace> namespaceCache = EvictableCacheBuilder.newBuilder()
                .expireAfterWrite(restCatalogConfig.getCaseInsensitiveNameMatchingCacheTtl().toMillis(), MILLISECONDS)
                .shareNothingWhenDisabled()
                .build();
        Cache<TableIdentifier, TableIdentifier> tableCache = EvictableCacheBuilder.newBuilder()
                .expireAfterWrite(restCatalogConfig.getCaseInsensitiveNameMatchingCacheTtl().toMillis(), MILLISECONDS)
                .shareNothingWhenDisabled()
                .build();

        return new TrinoRestCatalog(
                restSessionCatalog,
                CATALOG_NAME,
                SessionType.NONE,
                ImmutableMap.of(),
                false, // nestedNamespaceEnabled
                TRINO_VERSION,
                TYPE_MANAGER,
                false, // useUniqueTableLocation
                caseInsensitive,
                namespaceCache,
                tableCache,
                false); // viewEndpointsEnabled
    }
}
