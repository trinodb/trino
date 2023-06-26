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
package io.trino.plugin.pinot.auth;

import io.airlift.configuration.Config;
import jakarta.validation.constraints.NotNull;

public class PinotAuthenticationTypeConfig
{
    private PinotAuthenticationType controllerAuthenticationType = PinotAuthenticationType.NONE;
    private PinotAuthenticationType brokerAuthenticationType = PinotAuthenticationType.NONE;

    @NotNull
    public PinotAuthenticationType getControllerAuthenticationType()
    {
        return controllerAuthenticationType;
    }

    @Config("pinot.controller.authentication.type")
    public PinotAuthenticationTypeConfig setControllerAuthenticationType(PinotAuthenticationType controllerAuthenticationType)
    {
        this.controllerAuthenticationType = controllerAuthenticationType;
        return this;
    }

    @NotNull
    public PinotAuthenticationType getBrokerAuthenticationType()
    {
        return brokerAuthenticationType;
    }

    @Config("pinot.broker.authentication.type")
    public PinotAuthenticationTypeConfig setBrokerAuthenticationType(PinotAuthenticationType brokerAuthenticationType)
    {
        this.brokerAuthenticationType = brokerAuthenticationType;
        return this;
    }
}
