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
package io.trino.plugin.google.sheets;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.plugin.base.util.Closables;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.util.HashMap;
import java.util.Map;

import static io.trino.plugin.google.sheets.TestSheetsPlugin.TEST_METADATA_SHEET_ID;
import static io.trino.plugin.google.sheets.TestSheetsPlugin.getTestCredentialsPath;
import static io.trino.testing.TestingSession.testSessionBuilder;

public final class SheetsQueryRunner
{
    private SheetsQueryRunner() {}

    static final String GOOGLE_SHEETS = "gsheets";

    public static Builder builder()
            throws Exception
    {
        return new Builder()
                .addConnectorProperty("gsheets.credentials-path", getTestCredentialsPath())
                .addConnectorProperty("gsheets.max-data-cache-size", "1000")
                .addConnectorProperty("gsheets.data-cache-ttl", "1m");
    }

    public static class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final Map<String, String> connectorProperties = new HashMap<>();

        protected Builder()
        {
            super(testSessionBuilder()
                    .setCatalog(GOOGLE_SHEETS)
                    .setSchema("default")
                    .build());
        }

        @CanIgnoreReturnValue
        public Builder addConnectorProperty(String key, String value)
        {
            this.connectorProperties.put(key, value);
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();
            try {
                queryRunner.installPlugin(new SheetsPlugin());
                queryRunner.createCatalog(GOOGLE_SHEETS, GOOGLE_SHEETS, connectorProperties);

                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of());

                return queryRunner;
            }
            catch (Throwable e) {
                Closables.closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();

        QueryRunner queryRunner = builder()
                .addCoordinatorProperty("http-server.http.port", "8080")
                .addConnectorProperty("gsheets.metadata-sheet-id", TEST_METADATA_SHEET_ID)
                .build();

        Logger log = Logger.get(SheetsQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
