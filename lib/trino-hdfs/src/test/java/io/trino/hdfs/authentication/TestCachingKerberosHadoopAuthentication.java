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
package io.trino.hdfs.authentication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;

import java.net.InetAddress;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.security.UserGroupInformation.createUserGroupInformationForSubject;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCachingKerberosHadoopAuthentication
{
    @BeforeAll
    public static void setUp()
    {
        Configuration configuration = new Configuration(false);
        configuration.set("hadoop.security.authentication", "simple");
        UserGroupInformation.setConfiguration(configuration);
    }

    @Test
    public void testRefreshUpdatesCachedUserGroupInformationInPlace()
    {
        KerberosTicket expiredTicket = ticket("expired", Instant.now().minusSeconds(60), Instant.now().minusSeconds(30));
        KerberosTicket refreshedTicket = ticket("refreshed", Instant.now(), Instant.now().plusSeconds(60));
        TestingHadoopAuthentication delegate = new TestingHadoopAuthentication(expiredTicket, refreshedTicket);
        CachingKerberosHadoopAuthentication authentication = new CachingKerberosHadoopAuthentication(delegate);

        UserGroupInformation originalUserGroupInformation = authentication.getUserGroupInformation();

        assertThat(authentication.getUserGroupInformation()).isSameAs(originalUserGroupInformation);
        assertThat(delegate.getInvocations()).isEqualTo(2);
        assertThat(originalUserGroupInformation.getSubject().getPrivateCredentials(KerberosTicket.class))
                .containsExactly(refreshedTicket);
    }

    @Test
    public void testDoesNotRefreshBeforeRefreshTime()
    {
        KerberosTicket ticket = ticket("current", Instant.now(), Instant.now().plusSeconds(60));
        TestingHadoopAuthentication delegate = new TestingHadoopAuthentication(ticket, ticket("unused", Instant.now(), Instant.now().plusSeconds(120)));
        CachingKerberosHadoopAuthentication authentication = new CachingKerberosHadoopAuthentication(delegate);

        UserGroupInformation originalUserGroupInformation = authentication.getUserGroupInformation();

        assertThat(authentication.getUserGroupInformation()).isSameAs(originalUserGroupInformation);
        assertThat(delegate.getInvocations()).isEqualTo(1);
        assertThat(originalUserGroupInformation.getSubject().getPrivateCredentials(KerberosTicket.class))
                .containsExactly(ticket);
    }

    private static KerberosTicket ticket(String encodedValue, Instant start, Instant end)
    {
        KerberosPrincipal principal = new KerberosPrincipal("hdfs/hadoop-master@TRINO.TEST");
        return new KerberosTicket(
                encodedValue.getBytes(UTF_8),
                principal,
                new KerberosPrincipal("krbtgt/TRINO.TEST@TRINO.TEST"),
                new byte[8],
                1,
                new boolean[7],
                Date.from(start),
                Date.from(start),
                Date.from(end),
                Date.from(end),
                new InetAddress[0]);
    }

    private static UserGroupInformation userGroupInformation(KerberosTicket ticket)
    {
        Subject subject = new Subject();
        subject.getPrincipals().add(ticket.getClient());
        subject.getPrivateCredentials().add(ticket);
        return createUserGroupInformationForSubject(subject);
    }

    private static class TestingHadoopAuthentication
            implements HadoopAuthentication
    {
        private final KerberosTicket firstTicket;
        private final KerberosTicket refreshedTicket;
        private final AtomicInteger invocations = new AtomicInteger();

        private TestingHadoopAuthentication(KerberosTicket firstTicket, KerberosTicket refreshedTicket)
        {
            this.firstTicket = firstTicket;
            this.refreshedTicket = refreshedTicket;
        }

        @Override
        public UserGroupInformation getUserGroupInformation()
        {
            if (invocations.getAndIncrement() == 0) {
                return userGroupInformation(firstTicket);
            }
            return userGroupInformation(refreshedTicket);
        }

        public int getInvocations()
        {
            return invocations.get();
        }
    }
}
