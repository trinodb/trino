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
package org.apache.ranger.audit.provider.solr;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import org.apache.ranger.audit.destination.AuditDestination;
import org.apache.ranger.audit.model.AuditEventBase;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.Properties;

/**
 * Replacement for the class of the same name shipped in {@code ranger-audit-dest-solr},
 * which is used when the legacy {@code xasecure.audit.solr.is.enabled} property is set.
 * <p>
 * The class has to keep the upstream name, because Ranger instantiates it by its
 * fully qualified class name.
 *
 * @see org.apache.ranger.audit.destination.SolrAuditDestination
 */
// TODO: remove together with SolrAuditDestination
public class SolrAuditProvider
        extends AuditDestination
{
    private static final Logger log = Logger.get(SolrAuditProvider.class);

    public static final String AUDIT_SOLR_URL_PROP = "xasecure.audit.solr.solr_url";
    public static final String AUDIT_MAX_QUEUE_SIZE_PROP = "xasecure.audit.solr.async.max.queue.size";
    public static final String AUDIT_MAX_FLUSH_INTERVAL_PROP = "xasecure.audit.solr.async.max.flush.interval.ms";
    public static final String AUDIT_RETRY_WAIT_PROP = "xasecure.audit.solr.retry.ms";

    private static final int CONNECTION_TIMEOUT_MILLIS = 1000;

    private volatile SolrClient solrClient;
    private volatile long lastConnectTime;
    private volatile long lastFailTime;
    private volatile int retryWaitTime = 30000;

    @Override
    public void init(Properties props)
    {
        super.init(props);
        retryWaitTime = MiscUtil.getIntProperty(props, AUDIT_RETRY_WAIT_PROP, retryWaitTime);
    }

    @Override
    public void start()
    {
        connect();
    }

    @Override
    public void stop()
    {
        SolrClient solrClient = this.solrClient;
        this.solrClient = null;
        if (solrClient != null) {
            try {
                solrClient.close();
            }
            catch (IOException e) {
                log.error(e, "Error while closing Solr client");
            }
        }
    }

    @Override
    public boolean log(AuditEventBase event)
    {
        if (!(event instanceof AuthzAuditEvent authzEvent)) {
            log.error("%s audit event class type is not supported", event.getClass().getName());
            return false;
        }

        if (authzEvent.getAgentHostname() == null) {
            authzEvent.setAgentHostname(MiscUtil.getHostname());
        }
        if (authzEvent.getLogType() == null) {
            authzEvent.setLogType("RangerAudit");
        }
        if (authzEvent.getEventId() == null) {
            authzEvent.setEventId(MiscUtil.generateUniqueId());
        }

        try {
            if (solrClient == null) {
                connect();
            }
            SolrClient solrClient = this.solrClient;
            if (solrClient == null) {
                return false;
            }

            if (lastFailTime > 0 && System.currentTimeMillis() - lastFailTime < retryWaitTime) {
                // Solr rejected the previous event, wait before sending the next one
                return false;
            }

            Collection<SolrInputDocument> documents = ImmutableList.of(toSolrDocument(authzEvent));
            UpdateResponse response = MiscUtil.executePrivilegedAction((PrivilegedExceptionAction<UpdateResponse>) () -> solrClient.add(documents));
            if (response.getStatus() != 0) {
                lastFailTime = System.currentTimeMillis();
            }
            else {
                lastFailTime = 0;
            }
            return true;
        }
        catch (Throwable t) {
            log.error(t, "Error sending message to Solr");
            return false;
        }
    }

    @Override
    public boolean log(Collection<AuditEventBase> events)
    {
        for (AuditEventBase event : events) {
            log(event);
        }
        return true;
    }

    @Override
    public boolean logJSON(String event)
    {
        return log(MiscUtil.fromJson(event, AuthzAuditEvent.class));
    }

    @Override
    public boolean logJSON(Collection<String> events)
    {
        for (String event : events) {
            logJSON(event);
        }
        return false;
    }

    @Override
    public void waitToComplete() {}

    @Override
    public void waitToComplete(long timeout) {}

    @Override
    public void flush() {}

    public boolean isAsync()
    {
        return true;
    }

    private synchronized void connect()
    {
        if (solrClient != null) {
            return;
        }

        String solrUrl = MiscUtil.getStringProperty(props, AUDIT_SOLR_URL_PROP);
        long now = System.currentTimeMillis();
        if (lastConnectTime != 0 && now - lastConnectTime < retryWaitTime) {
            // wait before trying to connect again
            return;
        }
        lastConnectTime = now;

        if (solrUrl == null || solrUrl.isEmpty()) {
            log.error("Solr URL for audit is empty");
            return;
        }

        try {
            solrClient = MiscUtil.executePrivilegedAction((PrivilegedExceptionAction<SolrClient>) () ->
                    new HttpSolrClient.Builder()
                            .withBaseSolrUrl(solrUrl)
                            .allowCompression(true)
                            .withConnectionTimeout(CONNECTION_TIMEOUT_MILLIS)
                            .build());
        }
        catch (Throwable t) {
            log.error(t, "Can't connect to Solr server. URL=%s", solrUrl);
        }
    }

    private static SolrInputDocument toSolrDocument(AuthzAuditEvent event)
    {
        SolrInputDocument document = new SolrInputDocument();
        document.addField("id", event.getEventId());
        document.addField("access", event.getAccessType());
        document.addField("enforcer", event.getAclEnforcer());
        document.addField("agent", event.getAgentId());
        document.addField("repo", event.getRepositoryName());
        document.addField("sess", event.getSessionId());
        document.addField("reqUser", event.getUser());
        document.addField("reqData", event.getRequestData());
        document.addField("resource", event.getResourcePath());
        document.addField("cliIP", event.getClientIP());
        document.addField("logType", event.getLogType());
        document.addField("result", event.getAccessResult());
        document.addField("policy", event.getPolicyId());
        document.addField("repoType", event.getRepositoryType());
        document.addField("resType", event.getResourceType());
        document.addField("reason", event.getResultReason());
        document.addField("action", event.getAction());
        document.addField("evtTime", event.getEventTime());
        document.addField("tags", event.getTags());
        document.addField("cluster", event.getClusterName());
        document.addField("zone", event.getZoneName());
        document.addField("agentHost", event.getAgentHostname());
        return document;
    }
}
