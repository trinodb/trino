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
package io.trino.testing;

import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.Session.SessionBuilder;
import io.trino.connector.CatalogName;
import io.trino.connector.system.StaticSystemTablesProvider;
import io.trino.connector.system.SystemTablesMetadata;
import io.trino.execution.QueryIdGenerator;
import io.trino.metadata.Catalog;
import io.trino.metadata.Catalog.SecurityManagement;
import io.trino.metadata.SessionPropertyManager;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.security.Identity;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.spi.type.TimeZoneKey;
import io.trino.sql.SqlPath;

import java.util.Optional;

import static io.trino.connector.CatalogName.createInformationSchemaCatalogName;
import static io.trino.connector.CatalogName.createSystemTablesCatalogName;
import static java.util.Locale.ENGLISH;

public final class TestingSession
{
    private static final QueryIdGenerator queryIdGenerator = new QueryIdGenerator();

    /*
     * Pacific/Apia
     *  - has DST (e.g. January 2017)
     *    - DST backward change by 1 hour on 2017-04-02 04:00 local time
     *    - DST forward change by 1 hour on 2017-09-24 03:00 local time
     *  - had DST change at midnight (on Sunday, 26 September 2010, 00:00:00 clocks were turned forward 1 hour)
     *  - had offset change since 1970 (offset in January 1970: -11:00, offset in January 2017: +14:00, offset in June 2017: +13:00)
     *  - a whole day was skipped during policy change (on Friday, 30 December 2011, 00:00:00 clocks were turned forward 24 hours)
     */
    public static final TimeZoneKey DEFAULT_TIME_ZONE_KEY = TimeZoneKey.getTimeZoneKey("Pacific/Apia");

    private TestingSession() {}

    public static SessionBuilder testSessionBuilder()
    {
        return testSessionBuilder(new SessionPropertyManager());
    }

    public static SessionBuilder testSessionBuilder(SessionPropertyManager sessionPropertyManager)
    {
        return Session.builder(sessionPropertyManager)
                .setQueryId(queryIdGenerator.createNextQueryId())
                .setIdentity(Identity.ofUser("user"))
                .setSource("test")
                .setCatalog("catalog")
                .setSchema("schema")
                .setPath(new SqlPath(Optional.of("path")))
                .setTimeZoneKey(DEFAULT_TIME_ZONE_KEY)
                .setLocale(ENGLISH)
                .setRemoteUserAddress("address")
                .setUserAgent("agent");
    }

    public static Catalog createBogusTestingCatalog(String catalogName)
    {
        CatalogName catalog = new CatalogName(catalogName);
        return new Catalog(
                catalogName,
                catalog,
                createTestSessionConnector(),
                SecurityManagement.CONNECTOR,
                createInformationSchemaCatalogName(catalog),
                createTestSessionConnector(),
                createSystemTablesCatalogName(catalog),
                createTestSessionConnector());
    }

    private static Connector createTestSessionConnector()
    {
        return new Connector()
        {
            @Override
            public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
            {
                return new ConnectorTransactionHandle() {};
            }

            @Override
            public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction)
            {
                return new SystemTablesMetadata(new StaticSystemTablesProvider(ImmutableSet.of()));
            }
        };
    }
}
