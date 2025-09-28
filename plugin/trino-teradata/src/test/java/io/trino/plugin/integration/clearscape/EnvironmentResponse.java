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

package io.trino.plugin.integration.clearscape;

import java.util.List;

public record EnvironmentResponse(
        State state,
        String region,
        String name,
        String ip,
        String dnsName,
        String owner,
        String type,
        List<Service> services)
{
    public enum State
    {
        RUNNING,
        STOPPED,
    }

    record Service(
            List<Credential> credentials,
            String name,
            String url
    ) {}

    record Credential(
            String name,
            String value
    ) {}
}
