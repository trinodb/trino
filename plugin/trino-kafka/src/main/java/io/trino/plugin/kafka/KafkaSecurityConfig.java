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
package io.trino.plugin.kafka;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.AssertTrue;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import java.util.Optional;

import static org.apache.kafka.common.security.auth.SecurityProtocol.PLAINTEXT;
import static org.apache.kafka.common.security.auth.SecurityProtocol.SSL;

public class KafkaSecurityConfig
{
    private SecurityProtocol securityProtocol;

    public Optional<SecurityProtocol> getSecurityProtocol()
    {
        return Optional.ofNullable(securityProtocol);
    }

    @Config("kafka.security-protocol")
    @ConfigDescription("Kafka communication security protocol")
    public KafkaSecurityConfig setSecurityProtocol(SecurityProtocol securityProtocol)
    {
        this.securityProtocol = securityProtocol;
        return this;
    }

    @AssertTrue(message = "Only PLAINTEXT and SSL security protocols are supported. See 'kafka.config.resources' if other security protocols are needed")
    public boolean isValidSecurityProtocol()
    {
        return securityProtocol == null || securityProtocol.equals(PLAINTEXT) || securityProtocol.equals(SSL);
    }
}
