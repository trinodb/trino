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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.security.GroupProvider;

import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class TestingGroupProvider
        implements GroupProvider
{
    private Map<String, Set<String>> userGroups = ImmutableMap.of();

    public void reset()
    {
        setUserGroups(ImmutableMap.of());
    }

    public void setUserGroups(Map<String, Set<String>> userGroups)
    {
        this.userGroups = requireNonNull(userGroups, "userGroups is null");
    }

    @Override
    public Set<String> getGroups(String user)
    {
        return userGroups.getOrDefault(user, ImmutableSet.of());
    }
}
