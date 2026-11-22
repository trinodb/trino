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
package io.trino.plugin.ranger;

import com.google.common.collect.ImmutableList;
import org.apache.ranger.audit.destination.SolrAuditDestination;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.audit.provider.AuditHandler;
import org.apache.ranger.audit.provider.solr.SolrAuditProvider;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrDocument;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.util.Date;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
final class TestSolrAuditDestination
{
    private static final String COLLECTION = "ranger_audits";
    private static final int SOLR_PORT = 8983;

    private GenericContainer<?> solr;
    private SolrClient solrClient;

    @BeforeAll
    void setUp()
    {
        solr = new GenericContainer<>("solr:8.11.4")
                .withCommand("solr-precreate", COLLECTION)
                .withExposedPorts(SOLR_PORT)
                .waitingFor(Wait.forHttp("/solr/%s/admin/ping".formatted(COLLECTION)).forPort(SOLR_PORT));
        solr.start();
        solrClient = new HttpSolrClient.Builder(solrUrl()).build();
    }

    @AfterAll
    void tearDown()
            throws Exception
    {
        if (solrClient != null) {
            solrClient.close();
            solrClient = null;
        }
        if (solr != null) {
            solr.close();
            solr = null;
        }
    }

    @Test
    void testRangerLoadsClassesByName()
    {
        // Ranger's AuditProviderFactory instantiates both classes by these names
        assertThat(SolrAuditDestination.class.getName()).isEqualTo("org.apache.ranger.audit.destination.SolrAuditDestination");
        assertThat(SolrAuditProvider.class.getName()).isEqualTo("org.apache.ranger.audit.provider.solr.SolrAuditProvider");
    }

    @Test
    void testSolrJKerberosBuilderIsNotLoadable()
    {
        // The reason why the upstream Solr audit destination is replaced: SolrJ's Krb5HttpClientBuilder requires
        // Jetty 9-11 client classes. If it becomes loadable, an unsupported Jetty version is back on the classpath.
        assertThatThrownBy(() -> Class.forName("org.apache.solr.client.solrj.impl.Krb5HttpClientBuilder"))
                .isInstanceOf(NoClassDefFoundError.class)
                .hasMessageContaining("org/eclipse/jetty");
    }

    @Test
    void testAuditDestination()
            throws Exception
    {
        Properties properties = new Properties();
        properties.setProperty("xasecure.audit.destination.solr", "true");
        properties.setProperty("xasecure.audit.destination.solr.urls", solrUrl());

        AuthzAuditEvent event = createEvent();
        SolrAuditDestination destination = new SolrAuditDestination();
        try {
            destination.init(properties, "xasecure.audit.destination.solr");
            assertThat(destination.log(ImmutableList.of(event))).isTrue();
            assertThat(destination.getTotalSuccessCount()).isEqualTo(1);
        }
        finally {
            destination.stop();
        }

        SolrDocument document = solrClient.getById(event.getEventId());
        assertThat((Object) document).isNotNull();
        assertThat(document.getFirstValue("reqUser")).isEqualTo("alice");
        assertThat(document.getFirstValue("resource")).isEqualTo("tpch/tiny/nation");
        assertThat(document.getFirstValue("zoneName")).isEqualTo("test_zone");
    }

    @Test
    void testAuditDestinationWithoutSolr()
    {
        Properties properties = new Properties();
        properties.setProperty("xasecure.audit.destination.solr", "true");
        properties.setProperty("xasecure.audit.destination.solr.urls", "NONE");

        SolrAuditDestination destination = new SolrAuditDestination();
        try {
            destination.init(properties, "xasecure.audit.destination.solr");
            assertThat(destination.log(ImmutableList.of(createEvent()))).isFalse();
            assertThat(destination.getTotalDeferredCount()).isEqualTo(1);
        }
        finally {
            destination.stop();
        }
    }

    @Test
    void testLegacyAuditProvider()
            throws Exception
    {
        Properties properties = new Properties();
        properties.setProperty("xasecure.audit.solr.is.enabled", "true");
        properties.setProperty(SolrAuditProvider.AUDIT_SOLR_URL_PROP, solrUrl());

        AuthzAuditEvent event = createEvent();
        AuditHandler provider = new SolrAuditProvider();
        try {
            provider.init(properties);
            provider.start();
            assertThat(provider.log(event)).isTrue();
        }
        finally {
            provider.stop();
        }

        SolrDocument document = solrClient.getById(event.getEventId());
        assertThat((Object) document).isNotNull();
        assertThat(document.getFirstValue("reqUser")).isEqualTo("alice");
        assertThat(document.getFirstValue("zone")).isEqualTo("test_zone");
    }

    private String solrUrl()
    {
        return "http://%s:%s/solr/%s".formatted(solr.getHost(), solr.getMappedPort(SOLR_PORT), COLLECTION);
    }

    private static AuthzAuditEvent createEvent()
    {
        AuthzAuditEvent event = new AuthzAuditEvent();
        event.setEventId(UUID.randomUUID().toString());
        event.setEventTime(new Date());
        event.setUser("alice");
        event.setAccessType("select");
        event.setAccessResult((short) 1);
        event.setResourcePath("tpch/tiny/nation");
        event.setResourceType("table");
        event.setRepositoryName("dev_trino");
        event.setAclEnforcer("ranger-acl");
        event.setAgentId("trino");
        event.setLogType("RangerAudit");
        event.setZoneName("test_zone");
        return event;
    }
}
