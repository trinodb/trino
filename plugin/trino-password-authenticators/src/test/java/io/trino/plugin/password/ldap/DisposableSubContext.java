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

import javax.naming.directory.DirContext;

import static java.util.Objects.requireNonNull;

public class DisposableSubContext
        implements AutoCloseable
{
    private final String distinguishedName;

    private final TestingOpenLdapServerBase ldapServerBase;

    public DisposableSubContext(String distinguishedName, TestingOpenLdapServerBase testingOpenLdapServerBase)
    {
        this.distinguishedName = requireNonNull(distinguishedName, "distinguishedName is null");
        ldapServerBase = requireNonNull(testingOpenLdapServerBase, "testingOpenLdapServerBase is null");
    }

    public String getDistinguishedName()
    {
        return distinguishedName;
    }

    @Override
    public void close()
            throws Exception
    {
        DirContext context = ldapServerBase.createContext();
        context.destroySubcontext(distinguishedName);
        context.close();
    }
}
