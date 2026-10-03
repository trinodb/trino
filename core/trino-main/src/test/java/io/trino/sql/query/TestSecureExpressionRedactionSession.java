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
package io.trino.sql.query;

import io.trino.FeaturesConfig;
import io.trino.SystemSessionProperties;
import io.trino.execution.DynamicFilterConfig;
import io.trino.execution.QueryManagerConfig;
import io.trino.execution.TaskManagerConfig;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.memory.MemoryManagerConfig;
import io.trino.memory.NodeMemoryConfig;
import io.trino.metadata.SessionPropertyManager;
import io.trino.server.protocol.spooling.SpoolingEnabledConfig;
import io.trino.sql.planner.OptimizerConfig;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSecureExpressionRedactionSession
{
    @Test
    public void testServerEnabledRedactionCannotBeDisabled()
    {
        SessionPropertyManager manager = createSessionPropertyManager(
                new FeaturesConfig().setSecureExpressionRedactionEnabled(true));

        assertThatThrownBy(() -> manager.validateSystemSessionProperty(
                SystemSessionProperties.SECURE_EXPRESSION_REDACTION_ENABLED,
                "false"))
                .hasMessage("secure_expression_redaction_enabled cannot be disabled with session property when it was enabled with configuration");
        manager.validateSystemSessionProperty(SystemSessionProperties.SECURE_EXPRESSION_REDACTION_ENABLED, "true");
    }

    @Test
    public void testServerDisabledRedactionCanBeEnabledForDevelopment()
    {
        SessionPropertyManager manager = createSessionPropertyManager(new FeaturesConfig());

        manager.validateSystemSessionProperty(SystemSessionProperties.SECURE_EXPRESSION_REDACTION_ENABLED, "false");
        manager.validateSystemSessionProperty(SystemSessionProperties.SECURE_EXPRESSION_REDACTION_ENABLED, "true");
    }

    @Test
    public void testPropertyIsHidden()
    {
        assertThat(systemSessionProperties(new FeaturesConfig()).getSessionProperties().stream()
                .filter(property -> property.getName().equals(SystemSessionProperties.SECURE_EXPRESSION_REDACTION_ENABLED))
                .findFirst()
                .orElseThrow()
                .isHidden())
                .isTrue();
    }

    private static SessionPropertyManager createSessionPropertyManager(FeaturesConfig featuresConfig)
    {
        return new SessionPropertyManager(systemSessionProperties(featuresConfig));
    }

    private static SystemSessionProperties systemSessionProperties(FeaturesConfig featuresConfig)
    {
        return new SystemSessionProperties(
                new QueryManagerConfig(),
                new SpoolingEnabledConfig(),
                new TaskManagerConfig(),
                new MemoryManagerConfig(),
                featuresConfig,
                new OptimizerConfig(),
                new NodeMemoryConfig(),
                new DynamicFilterConfig(),
                new NodeSchedulerConfig());
    }
}
