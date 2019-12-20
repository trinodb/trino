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
package io.prestosql.dispatcher;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;

import static com.google.common.net.HttpHeaders.X_FORWARDED_PROTO;

public class DispatcherConfig
{
    public enum HeaderSupport
    {
        WARN,
        IGNORE,
        ACCEPT,
        /**/;
    }

    // When Presto is not behind a load-balancer, accepting user-provided X-Forwarded-For would be not be safe.
    private HeaderSupport forwardedHeaderSupport = HeaderSupport.WARN;

    @NotNull
    public HeaderSupport getForwardedHeaderSupport()
    {
        return forwardedHeaderSupport;
    }

    @Config("dispatcher.forwarded-header")
    @ConfigDescription("Support for " + X_FORWARDED_PROTO + " header")
    public DispatcherConfig setForwardedHeaderSupport(HeaderSupport forwardedHeaderSupport)
    {
        this.forwardedHeaderSupport = forwardedHeaderSupport;
        return this;
    }
}
