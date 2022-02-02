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
package io.trino.plugin.password.ldap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class LdapObjectDefinition
{
    private final String id;
    private final String distinguishedName;
    private final Map<String, String> attributes;
    private final List<String> objectClasses;

    private LdapObjectDefinition(String id, String distinguishedName, Map<String, String> attributes, List<String> objectClasses)
    {
        this.id = requireNonNull(id, "id is null");
        this.distinguishedName = requireNonNull(distinguishedName, "distinguishedName is null");
        this.attributes = ImmutableMap.copyOf(requireNonNull(attributes, "attributes is null"));
        this.objectClasses = ImmutableList.copyOf(requireNonNull(objectClasses, "objectClasses is null"));
    }

    public static LdapObjectDefinitionBuilder builder(String id)
    {
        return new LdapObjectDefinitionBuilder(id);
    }

    public List<String> getObjectClasses()
    {
        return objectClasses;
    }

    public Map<String, String> getAttributes()
    {
        return attributes;
    }

    public String getId()
    {
        return id;
    }

    public String getDistinguishedName()
    {
        return distinguishedName;
    }

    public static class LdapObjectDefinitionBuilder
    {
        private String id;
        private String distinguishedName;
        private List<String> objectClasses;
        private Map<String, String> attributes;

        private LdapObjectDefinitionBuilder(String id)
        {
            this.id = requireNonNull(id, "id is null");
        }

        public LdapObjectDefinition build()
        {
            return new LdapObjectDefinition(id, distinguishedName, attributes, objectClasses);
        }

        public LdapObjectDefinitionBuilder setDistinguishedName(String distinguishedName)
        {
            this.distinguishedName = distinguishedName;
            return this;
        }

        public LdapObjectDefinitionBuilder setAttributes(Map<String, String> attributes)
        {
            this.attributes = ImmutableMap.copyOf(attributes);
            return this;
        }

        public LdapObjectDefinitionBuilder setObjectClasses(List<String> objectClasses)
        {
            this.objectClasses = ImmutableList.copyOf(objectClasses);
            return this;
        }
    }
}
