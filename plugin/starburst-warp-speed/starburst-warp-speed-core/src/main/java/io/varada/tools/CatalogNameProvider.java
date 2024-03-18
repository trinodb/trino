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
package io.varada.tools;

import com.google.inject.Provider;

public class CatalogNameProvider
        implements Provider<String>
{
    private final String connectorName;

    public CatalogNameProvider(String connectorName)
    {
        this.connectorName = connectorName;
    }

    @Override
    public String get()
    {
        return connectorName;
    }

    public String getRestTaskPrefix()
    {
        return "/v1/ext/" + get();
    }
}
