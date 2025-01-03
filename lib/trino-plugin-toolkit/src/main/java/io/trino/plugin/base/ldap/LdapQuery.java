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
package io.trino.plugin.base.ldap;

import java.util.Arrays;

import static java.util.Objects.requireNonNull;

public class LdapQuery
{
    private final String searchBase;
    private final String searchFilter;
    private final Object[] filterArguments;
    private final String[] attributes;

    private LdapQuery(String searchBase, String searchFilter, Object[] filterArguments, String[] attributes)
    {
        this.searchBase = requireNonNull(searchBase, "searchBase is null");
        this.searchFilter = requireNonNull(searchFilter, "searchFilter is null");
        requireNonNull(attributes, "attributes is null");
        requireNonNull(filterArguments, "filterArguments is null");
        this.filterArguments = Arrays.copyOf(filterArguments, filterArguments.length);
        this.attributes = Arrays.copyOf(attributes, attributes.length);
    }

    public String getSearchBase()
    {
        return searchBase;
    }

    public String getSearchFilter()
    {
        return searchFilter;
    }

    public Object[] getFilterArguments()
    {
        return filterArguments;
    }

    public String[] getAttributes()
    {
        return attributes;
    }

    public static class LdapQueryBuilder
    {
        private String searchBase;
        private String searchFilter;
        private String[] attributes = new String[0];
        private Object[] filterArguments = new Object[0];

        public LdapQueryBuilder withSearchBase(String searchBase)
        {
            this.searchBase = requireNonNull(searchBase, "searchBase is null");
            return this;
        }

        public LdapQueryBuilder withSearchFilter(String searchFilter)
        {
            this.searchFilter = requireNonNull(searchFilter, "searchFilter is null");
            return this;
        }

        public LdapQueryBuilder withFilterArguments(Object... arguments)
        {
            this.filterArguments = requireNonNull(arguments, "arguments is null");
            return this;
        }

        public LdapQueryBuilder withAttributes(String... attributes)
        {
            this.attributes = requireNonNull(attributes, "attributes is null");
            return this;
        }

        public LdapQuery build()
        {
            return new LdapQuery(searchBase, searchFilter, filterArguments, attributes);
        }
    }
}
