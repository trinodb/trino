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
package io.trino.plugin.ldapgroup;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.trino.plugin.base.ldap.LdapClient;
import io.trino.plugin.base.ldap.LdapQuery;
import org.junit.jupiter.api.Test;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.SearchResult;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.ldapgroup.LdapFilteringGroupProviderConfig.LdapGroupSearchMode.MATCHING_RULE_IN_CHAIN;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMatchingRuleInChainLdapGroupResolver
{
    private static final String ADMIN_USER = "cn=admin,dc=example,dc=com";
    private static final String ADMIN_PASSWORD = "password";
    private static final String GROUP_BASE_DN = "ou=groups,dc=example,dc=com";
    private static final String USER_DN = "cn=alice,ou=users,dc=example,dc=com";

    @Test
    public void testMatchingRuleSearch()
    {
        RecordingLdapClient client = new RecordingLdapClient(ImmutableList.of(
                group("cn=engineering," + GROUP_BASE_DN, Optional.of("engineering")),
                group("cn=missing-name," + GROUP_BASE_DN, Optional.empty())));

        LdapGroupResolver resolver = createResolver(client, Optional.empty());

        assertThat(resolver.resolveGroups(USER_DN)).containsExactlyInAnyOrder(
                new LdapGroup("cn=engineering," + GROUP_BASE_DN, "engineering"),
                new LdapGroup("cn=missing-name," + GROUP_BASE_DN, "cn=missing-name," + GROUP_BASE_DN));
        assertThat(client.queries).hasSize(1);
        LdapQuery query = client.queries.getFirst();
        assertThat(query.getSearchFilter()).isEqualTo("(member:1.2.840.113556.1.4.1941:={0})");
        assertThat(query.getFilterArguments()).containsExactly(USER_DN);
        assertThat(query.getSearchBase()).isEqualTo(GROUP_BASE_DN);
        assertThat(query.getAttributes()).containsExactly("cn");
        assertThat(client.userNames).containsExactly(ADMIN_USER);
        assertThat(client.passwords).containsExactly(ADMIN_PASSWORD);
    }

    @Test
    public void testMatchingRuleSearchWithGroupFilter()
    {
        RecordingLdapClient client = new RecordingLdapClient(ImmutableList.of(group("cn=engineering," + GROUP_BASE_DN, Optional.of("engineering"))));

        assertThat(createResolver(client, Optional.of("cn=eng*")).resolveGroups(USER_DN))
                .containsExactly(new LdapGroup("cn=engineering," + GROUP_BASE_DN, "engineering"));
        assertThat(client.queries).singleElement()
                .extracting(LdapQuery::getSearchFilter)
                .isEqualTo("(&(cn=eng*)(member:1.2.840.113556.1.4.1941:={0}))");
    }

    @Test
    public void testEmptyMatchingRuleSearch()
    {
        RecordingLdapClient client = new RecordingLdapClient(ImmutableList.of());

        assertThat(createResolver(client, Optional.empty()).resolveGroups(USER_DN)).isEmpty();
        assertThat(client.queries).hasSize(1);
    }

    @Test
    public void testMatchingRuleSearchExceptionReturnsEmpty()
    {
        RecordingLdapClient client = new RecordingLdapClient(new NamingException("search failed"));

        assertThat(createResolver(client, Optional.empty()).resolveGroups(USER_DN)).isEmpty();
        assertThat(client.queries).hasSize(1);
    }

    @Test
    public void testModuleSelectsMatchingRuleResolver()
    {
        RecordingLdapClient client = new RecordingLdapClient(ImmutableList.of());
        Injector injector = new Bootstrap(
                new LdapGroupProviderModule(),
                binder -> binder.bind(LdapClient.class).toInstance(client))
                .doNotInitializeLogging()
                .disableSystemProperties()
                .setRequiredConfigurationProperties(ImmutableMap.<String, String>builder()
                        .put("ldap.admin-user", ADMIN_USER)
                        .put("ldap.admin-password", ADMIN_PASSWORD)
                        .put("ldap.user-base-dn", "ou=users,dc=example,dc=com")
                        .put("ldap.use-group-filter", "true")
                        .put("ldap.group-base-dn", GROUP_BASE_DN)
                        .put("ldap.group-search-mode", MATCHING_RULE_IN_CHAIN.name())
                        .buildOrThrow())
                .initialize();

        assertThat(injector.getInstance(LdapGroupResolver.class)).isInstanceOf(MatchingRuleInChainLdapGroupResolver.class);
    }

    private static LdapGroupResolver createResolver(RecordingLdapClient client, Optional<String> groupSearchFilter)
    {
        LdapGroupProviderConfig config = new LdapGroupProviderConfig()
                .setLdapAdminUser(ADMIN_USER)
                .setLdapAdminPassword(ADMIN_PASSWORD)
                .setLdapGroupsNameAttribute("cn");
        LdapFilteringGroupProviderConfig filteringConfig = new LdapFilteringGroupProviderConfig()
                .setLdapGroupBaseDN(GROUP_BASE_DN)
                .setLdapGroupsSearchMemberAttribute("member");
        groupSearchFilter.ifPresent(filteringConfig::setLdapGroupsSearchFilter);
        return new MatchingRuleInChainLdapGroupResolver(new LdapGroupSearcher(client, config, filteringConfig), filteringConfig);
    }

    private static SearchResult group(String distinguishedName, Optional<String> name)
    {
        BasicAttributes attributes = new BasicAttributes();
        name.ifPresent(value -> attributes.put(new BasicAttribute("cn", value)));
        SearchResult result = new SearchResult("", null, attributes);
        result.setNameInNamespace(distinguishedName);
        return result;
    }

    private static final class RecordingLdapClient
            implements LdapClient
    {
        private final List<SearchResult> results;
        private final NamingException exception;
        private final List<String> userNames = new ArrayList<>();
        private final List<String> passwords = new ArrayList<>();
        private final List<LdapQuery> queries = new ArrayList<>();

        private RecordingLdapClient(List<SearchResult> results)
        {
            this.results = ImmutableList.copyOf(results);
            this.exception = null;
        }

        private RecordingLdapClient(NamingException exception)
        {
            this.results = ImmutableList.of();
            this.exception = exception;
        }

        @Override
        public <T> T processLdapContext(String userName, String password, LdapContextProcessor<T> contextProcessor)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T executeLdapQuery(String userName, String password, LdapQuery ldapQuery, LdapSearchResultProcessor<T> resultProcessor)
                throws NamingException
        {
            userNames.add(userName);
            passwords.add(password);
            queries.add(ldapQuery);
            if (exception != null) {
                throw exception;
            }
            return resultProcessor.process(new ListNamingEnumeration(results));
        }
    }

    private static final class ListNamingEnumeration
            implements NamingEnumeration<SearchResult>
    {
        private final Iterator<SearchResult> iterator;

        private ListNamingEnumeration(List<SearchResult> results)
        {
            iterator = results.iterator();
        }

        @Override
        public SearchResult next()
        {
            return iterator.next();
        }

        @Override
        public boolean hasMore()
        {
            return iterator.hasNext();
        }

        @Override
        public void close() {}

        @Override
        public boolean hasMoreElements()
        {
            return hasMore();
        }

        @Override
        public SearchResult nextElement()
        {
            return next();
        }
    }
}
