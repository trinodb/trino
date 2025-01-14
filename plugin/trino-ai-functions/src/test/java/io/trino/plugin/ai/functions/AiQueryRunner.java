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
package io.trino.plugin.ai.functions;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import static io.trino.testing.TestingSession.testSessionBuilder;

public final class AiQueryRunner
{
    private AiQueryRunner() {}

    public static void main(String[] args)
            throws Exception
    {
        QueryRunner queryRunner = DistributedQueryRunner.builder(testSessionBuilder().build())
                .addCoordinatorProperty("http-server.http.port", "8080")
                .addCoordinatorProperty("sql.path", "ai.ai")
                .build();
        queryRunner.installPlugin(new AiPlugin());
        queryRunner.createCatalog("ai", "ai", ImmutableMap.<String, String>builder()
                .put("ai.model", "llama3.3")
                .put("ai.ollama.endpoint", "http://localhost:11434")
                .buildOrThrow());
        Logger log = Logger.get(AiQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
