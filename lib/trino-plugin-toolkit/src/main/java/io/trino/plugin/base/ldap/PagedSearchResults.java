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

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.Control;
import javax.naming.ldap.LdapContext;
import javax.naming.ldap.PagedResultsControl;
import javax.naming.ldap.PagedResultsResponseControl;

import java.io.IOException;
import java.util.NoSuchElementException;

import static java.util.Objects.requireNonNull;

/**
 * Holds the {@link SearchResult} entries collected from every page of an LDAP paged search
 * (RFC 2696) and exposes them to result processors as a single {@link NamingEnumeration}.
 */
class PagedSearchResults
        implements NamingEnumeration<SearchResult>
{
    private final LdapContext context;
    private final int pageSize;
    private final LdapQuery ldapQuery;
    private final SearchControls searchControls;

    private NamingEnumeration<SearchResult> currentPage;
    private byte[] cookie;
    private boolean firstPageFetched;

    public PagedSearchResults(LdapContext context, int pageSize, LdapQuery ldapQuery, SearchControls searchControls)
    {
        this.context = requireNonNull(context, "context is null");
        this.pageSize = pageSize;
        this.ldapQuery = requireNonNull(ldapQuery, "ldapQuery is null");
        this.searchControls = requireNonNull(searchControls, "searchControls is null");
    }

    @Override
    public boolean hasMore()
            throws NamingException
    {
        if (currentPage != null && currentPage.hasMore()) {
            return true;
        }
        if (firstPageFetched) {
            cookie = findCookie(responseControls());
            if (cookie == null) {
                return false;
            }
        }
        fetchNextPage();
        return hasMore();
    }

    @Override
    public SearchResult next()
            throws NamingException
    {
        if (!hasMore()) {
            throw new NoSuchElementException();
        }
        return currentPage.next();
    }

    @Override
    public boolean hasMoreElements()
    {
        try {
            return hasMore();
        }
        catch (NamingException e) {
            return false;
        }
    }

    @Override
    public SearchResult nextElement()
    {
        try {
            return next();
        }
        catch (NamingException e) {
            throw new NoSuchElementException(e);
        }
    }

    @Override
    public void close()
            throws NamingException
    {
        if (currentPage != null) {
            currentPage.close();
        }
    }

    @SuppressWarnings("BanJNDI")
    private void fetchNextPage()
            throws NamingException
    {
        try {
            context.setRequestControls(new Control[] {new PagedResultsControl(pageSize, cookie, Control.NONCRITICAL)});
        }
        catch (IOException e) {
            NamingException namingException = new NamingException("Failed to create LDAP paged results control");
            namingException.initCause(e);
            throw namingException;
        }
        currentPage = context.search(
                ldapQuery.getSearchBase(),
                ldapQuery.getSearchFilter(),
                ldapQuery.getFilterArguments(),
                searchControls);
        firstPageFetched = true;
    }

    @SuppressWarnings("BanJNDI")
    private Control[] responseControls()
            throws NamingException
    {
        return context.getResponseControls();
    }

    private static byte[] findCookie(Control[] responseControls)
    {
        if (responseControls == null) {
            return null;
        }
        for (Control control : responseControls) {
            if (control instanceof PagedResultsResponseControl pagedResultsResponseControl) {
                byte[] cookie = pagedResultsResponseControl.getCookie();
                if (cookie == null || cookie.length == 0) {
                    return null;
                }
                return cookie;
            }
        }
        return null;
    }
}
