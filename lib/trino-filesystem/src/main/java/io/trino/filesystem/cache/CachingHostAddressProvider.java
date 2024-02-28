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
package io.trino.filesystem.cache;

import io.trino.spi.HostAddress;

import java.util.List;

public interface CachingHostAddressProvider
{
    /**
     * Returns a lists of hosts which are preferred to cache the split with the given path.
     */
    List<HostAddress> getHosts(String splitPath, List<HostAddress> defaultAddresses);
}
