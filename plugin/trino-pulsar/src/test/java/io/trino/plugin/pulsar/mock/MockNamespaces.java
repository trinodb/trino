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
package io.trino.plugin.pulsar.mock;

import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.AutoSubscriptionCreationOverride;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BookieAffinityGroupData;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SchemaAutoUpdateCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.SubscribeRate;
import org.apache.pulsar.common.policies.data.SubscriptionAuthMode;

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.core.Response;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class MockNamespaces
        implements Namespaces
{
    private List<TopicName> topicNames;

    public MockNamespaces(List<TopicName> topicNames)
    {
        this.topicNames = topicNames;
    }

    @Override
    public List<String> getNamespaces(String tenant) throws PulsarAdminException
    {
        List<String> ns = topicNames.stream()
                .filter(topicName -> topicName.getTenant().equals(tenant))
                .map(TopicName::getNamespace)
                .distinct()
                .collect(Collectors.toCollection(LinkedList::new));
        if (ns.isEmpty()) {
            throw new PulsarAdminException(new ClientErrorException(Response.status(404).build()));
        }
        return ns;
    }

    @Override
    public void removeDeduplicationStatus(String namespace) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> removeDeduplicationStatusAsync(String namespace)
    {
        return null;
    }

    @Override
    public Boolean getDeduplicationStatus(String namespace) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<Boolean> getDeduplicationStatusAsync(String namespace)
    {
        return null;
    }

    @Override
    public CompletableFuture<List<String>> getNamespacesAsync(String s)
    {
        return null;
    }

    @Override
    public List<String> getNamespaces(String s, String s1) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public List<String> getTopics(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<List<String>> getTopicsAsync(String s)
    {
        return null;
    }

    @Override
    public BundlesData getBundles(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<BundlesData> getBundlesAsync(String s)
    {
        return null;
    }

    @Override
    public Policies getPolicies(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<Policies> getPoliciesAsync(String s)
    {
        return null;
    }

    @Override
    public void createNamespace(String s, int i) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> createNamespaceAsync(String s, int i)
    {
        return null;
    }

    @Override
    public void createNamespace(String s, BundlesData bundlesData) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> createNamespaceAsync(String s, BundlesData bundlesData)
    {
        return null;
    }

    @Override
    public void createNamespace(String s) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> createNamespaceAsync(String s)
    {
        return null;
    }

    @Override
    public void createNamespace(String s, Set<String> set) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> createNamespaceAsync(String s, Set<String> set)
    {
        return null;
    }

    @Override
    public void createNamespace(String s, Policies policies) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> createNamespaceAsync(String s, Policies policies)
    {
        return null;
    }

    @Override
    public void deleteNamespace(String s) throws PulsarAdminException
    { }

    @Override
    public void deleteNamespace(String s, boolean b) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> deleteNamespaceAsync(String s)
    {
        return null;
    }

    @Override
    public CompletableFuture<Void> deleteNamespaceAsync(String s, boolean b)
    {
        return null;
    }

    @Override
    public void deleteNamespaceBundle(String s, String s1) throws PulsarAdminException
    { }

    @Override
    public void deleteNamespaceBundle(String s, String s1, boolean b) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> deleteNamespaceBundleAsync(String s, String s1)
    {
        return null;
    }

    @Override
    public CompletableFuture<Void> deleteNamespaceBundleAsync(String s, String s1, boolean b)
    {
        return null;
    }

    @Override
    public Map<String, Set<AuthAction>> getPermissions(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<Map<String, Set<AuthAction>>> getPermissionsAsync(String s)
    {
        return null;
    }

    @Override
    public void grantPermissionOnNamespace(String s, String s1, Set<AuthAction> set) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> grantPermissionOnNamespaceAsync(String s, String s1, Set<AuthAction> set)
    {
        return null;
    }

    @Override
    public void revokePermissionsOnNamespace(String s, String s1) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> revokePermissionsOnNamespaceAsync(String s, String s1)
    {
        return null;
    }

    @Override
    public void grantPermissionOnSubscription(String s, String s1, Set<String> set) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> grantPermissionOnSubscriptionAsync(String s, String s1, Set<String> set)
    {
        return null;
    }

    @Override
    public void revokePermissionOnSubscription(String s, String s1, String s2) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> revokePermissionOnSubscriptionAsync(String s, String s1, String s2)
    {
        return null;
    }

    @Override
    public List<String> getNamespaceReplicationClusters(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<List<String>> getNamespaceReplicationClustersAsync(String s)
    {
        return null;
    }

    @Override
    public void setNamespaceReplicationClusters(String s, Set<String> set) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setNamespaceReplicationClustersAsync(String s, Set<String> set)
    {
        return null;
    }

    @Override
    public Integer getNamespaceMessageTTL(String s) throws PulsarAdminException
    {
        return 0;
    }

    @Override
    public CompletableFuture<Integer> getNamespaceMessageTTLAsync(String s)
    {
        return null;
    }

    @Override
    public void setNamespaceMessageTTL(String s, int i) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setNamespaceMessageTTLAsync(String s, int i)
    {
        return null;
    }

    @Override
    public void removeNamespaceMessageTTL(String s) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> removeNamespaceMessageTTLAsync(String s)
    {
        return null;
    }

    @Override
    public int getSubscriptionExpirationTime(String s) throws PulsarAdminException
    {
        return 0;
    }

    @Override
    public CompletableFuture<Integer> getSubscriptionExpirationTimeAsync(String s)
    {
        return null;
    }

    @Override
    public void setSubscriptionExpirationTime(String s, int i) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setSubscriptionExpirationTimeAsync(String s, int i)
    {
        return null;
    }

    @Override
    public void setNamespaceAntiAffinityGroup(String s, String s1) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setNamespaceAntiAffinityGroupAsync(String s, String s1)
    {
        return null;
    }

    @Override
    public List<String> getAntiAffinityNamespaces(String s, String s1, String s2) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<List<String>> getAntiAffinityNamespacesAsync(String s, String s1, String s2)
    {
        return null;
    }

    @Override
    public String getNamespaceAntiAffinityGroup(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<String> getNamespaceAntiAffinityGroupAsync(String s)
    {
        return null;
    }

    @Override
    public void deleteNamespaceAntiAffinityGroup(String s) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> deleteNamespaceAntiAffinityGroupAsync(String s)
    {
        return null;
    }

    @Override
    public void setDeduplicationStatus(String s, boolean b) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setDeduplicationStatusAsync(String s, boolean b)
    {
        return null;
    }

    @Override
    public void setAutoTopicCreation(String s, AutoTopicCreationOverride autoTopicCreationOverride) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setAutoTopicCreationAsync(String s, AutoTopicCreationOverride autoTopicCreationOverride)
    {
        return null;
    }

    @Override
    public void removeAutoTopicCreation(String s) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> removeAutoTopicCreationAsync(String s)
    {
        return null;
    }

    @Override
    public void setAutoSubscriptionCreation(String s, AutoSubscriptionCreationOverride autoSubscriptionCreationOverride) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setAutoSubscriptionCreationAsync(String s, AutoSubscriptionCreationOverride autoSubscriptionCreationOverride)
    {
        return null;
    }

    @Override
    public void setSubscriptionTypesEnabled(String namespace, Set<SubscriptionType> subscriptionTypesEnabled) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setSubscriptionTypesEnabledAsync(String namespace, Set<SubscriptionType> subscriptionTypesEnabled)
    {
        return null;
    }

    @Override
    public Set<SubscriptionType> getSubscriptionTypesEnabled(String namespace) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<Set<SubscriptionType>> getSubscriptionTypesEnabledAsync(String namespace)
    {
        return null;
    }

    @Override
    public void removeAutoSubscriptionCreation(String s) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> removeAutoSubscriptionCreationAsync(String s)
    {
        return null;
    }

    @Override
    public Map<BacklogQuota.BacklogQuotaType, BacklogQuota> getBacklogQuotaMap(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<Map<BacklogQuota.BacklogQuotaType, BacklogQuota>> getBacklogQuotaMapAsync(String s)
    {
        return null;
    }

    @Override
    public void setBacklogQuota(String s, BacklogQuota backlogQuota) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setBacklogQuotaAsync(String s, BacklogQuota backlogQuota)
    {
        return null;
    }

    @Override
    public void removeBacklogQuota(String s) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> removeBacklogQuotaAsync(String s)
    {
        return null;
    }

    @Override
    public void removePersistence(String namespace) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> removePersistenceAsync(String namespace)
    {
        return null;
    }

    @Override
    public void setPersistence(String s, PersistencePolicies persistencePolicies) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setPersistenceAsync(String s, PersistencePolicies persistencePolicies)
    {
        return null;
    }

    @Override
    public PersistencePolicies getPersistence(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<PersistencePolicies> getPersistenceAsync(String s)
    {
        return null;
    }

    @Override
    public void setBookieAffinityGroup(String s, BookieAffinityGroupData bookieAffinityGroupData) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setBookieAffinityGroupAsync(String s, BookieAffinityGroupData bookieAffinityGroupData)
    {
        return null;
    }

    @Override
    public void deleteBookieAffinityGroup(String s) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> deleteBookieAffinityGroupAsync(String s)
    {
        return null;
    }

    @Override
    public BookieAffinityGroupData getBookieAffinityGroup(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<BookieAffinityGroupData> getBookieAffinityGroupAsync(String s)
    {
        return null;
    }

    @Override
    public void setRetention(String s, RetentionPolicies retentionPolicies) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setRetentionAsync(String s, RetentionPolicies retentionPolicies)
    {
        return null;
    }

    @Override
    public void removeRetention(String namespace) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> removeRetentionAsync(String namespace)
    {
        return null;
    }

    @Override
    public RetentionPolicies getRetention(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<RetentionPolicies> getRetentionAsync(String s)
    {
        return null;
    }

    @Override
    public void unload(String s) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> unloadAsync(String s)
    {
        return null;
    }

    @Override
    public String getReplicationConfigVersion(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<String> getReplicationConfigVersionAsync(String s)
    {
        return null;
    }

    @Override
    public void unloadNamespaceBundle(String s, String s1) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> unloadNamespaceBundleAsync(String s, String s1)
    {
        return null;
    }

    @Override
    public void splitNamespaceBundle(String s, String s1, boolean b, String s2) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> splitNamespaceBundleAsync(String s, String s1, boolean b, String s2)
    {
        return null;
    }

    @Override
    public void setPublishRate(String s, PublishRate publishRate) throws PulsarAdminException
    { }

    @Override
    public void removePublishRate(String s) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setPublishRateAsync(String s, PublishRate publishRate)
    {
        return null;
    }

    @Override
    public CompletableFuture<Void> removePublishRateAsync(String s)
    {
        return null;
    }

    @Override
    public PublishRate getPublishRate(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<PublishRate> getPublishRateAsync(String s)
    {
        return null;
    }

    @Override
    public void removeDispatchRate(String namespace) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> removeDispatchRateAsync(String namespace)
    {
        return null;
    }

    @Override
    public void setDispatchRate(String s, DispatchRate dispatchRate) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setDispatchRateAsync(String s, DispatchRate dispatchRate)
    {
        return null;
    }

    @Override
    public DispatchRate getDispatchRate(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<DispatchRate> getDispatchRateAsync(String s)
    {
        return null;
    }

    @Override
    public void setSubscribeRate(String s, SubscribeRate subscribeRate) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setSubscribeRateAsync(String s, SubscribeRate subscribeRate)
    {
        return null;
    }

    @Override
    public void removeSubscribeRate(String namespace) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> removeSubscribeRateAsync(String namespace)
    {
        return null;
    }

    @Override
    public SubscribeRate getSubscribeRate(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<SubscribeRate> getSubscribeRateAsync(String s)
    {
        return null;
    }

    @Override
    public void removeSubscriptionDispatchRate(String namespace) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> removeSubscriptionDispatchRateAsync(String namespace)
    {
        return null;
    }

    @Override
    public void setSubscriptionDispatchRate(String s, DispatchRate dispatchRate) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setSubscriptionDispatchRateAsync(String s, DispatchRate dispatchRate)
    {
        return null;
    }

    @Override
    public DispatchRate getSubscriptionDispatchRate(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<DispatchRate> getSubscriptionDispatchRateAsync(String s)
    {
        return null;
    }

    @Override
    public void setReplicatorDispatchRate(String s, DispatchRate dispatchRate) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setReplicatorDispatchRateAsync(String s, DispatchRate dispatchRate)
    {
        return null;
    }

    @Override
    public void removeReplicatorDispatchRate(String namespace) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> removeReplicatorDispatchRateAsync(String namespace)
    {
        return null;
    }

    @Override
    public DispatchRate getReplicatorDispatchRate(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<DispatchRate> getReplicatorDispatchRateAsync(String s)
    {
        return null;
    }

    @Override
    public void clearNamespaceBacklog(String s) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> clearNamespaceBacklogAsync(String s)
    {
        return null;
    }

    @Override
    public void clearNamespaceBacklogForSubscription(String s, String s1) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> clearNamespaceBacklogForSubscriptionAsync(String s, String s1)
    {
        return null;
    }

    @Override
    public void clearNamespaceBundleBacklog(String s, String s1) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> clearNamespaceBundleBacklogAsync(String s, String s1)
    {
        return null;
    }

    @Override
    public void clearNamespaceBundleBacklogForSubscription(String s, String s1, String s2) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> clearNamespaceBundleBacklogForSubscriptionAsync(String s, String s1, String s2)
    {
        return null;
    }

    @Override
    public void unsubscribeNamespace(String s, String s1) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> unsubscribeNamespaceAsync(String s, String s1)
    {
        return null;
    }

    @Override
    public void unsubscribeNamespaceBundle(String s, String s1, String s2) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> unsubscribeNamespaceBundleAsync(String s, String s1, String s2)
    {
        return null;
    }

    @Override
    public void setEncryptionRequiredStatus(String s, boolean b) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setEncryptionRequiredStatusAsync(String s, boolean b)
    {
        return null;
    }

    @Override
    public DelayedDeliveryPolicies getDelayedDelivery(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<DelayedDeliveryPolicies> getDelayedDeliveryAsync(String s)
    {
        return null;
    }

    @Override
    public void setDelayedDeliveryMessages(String s, DelayedDeliveryPolicies delayedDeliveryPolicies) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setDelayedDeliveryMessagesAsync(String s, DelayedDeliveryPolicies delayedDeliveryPolicies)
    {
        return null;
    }

    @Override
    public void removeDelayedDeliveryMessages(String namespace) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> removeDelayedDeliveryMessagesAsync(String namespace)
    {
        return null;
    }

    @Override
    public InactiveTopicPolicies getInactiveTopicPolicies(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<Void> removeInactiveTopicPoliciesAsync(String s)
    {
        return null;
    }

    @Override
    public void removeInactiveTopicPolicies(String s) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<InactiveTopicPolicies> getInactiveTopicPoliciesAsync(String s)
    {
        return null;
    }

    @Override
    public void setInactiveTopicPolicies(String s, InactiveTopicPolicies inactiveTopicPolicies) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setInactiveTopicPoliciesAsync(String s, InactiveTopicPolicies inactiveTopicPolicies)
    {
        return null;
    }

    @Override
    public void setSubscriptionAuthMode(String s, SubscriptionAuthMode subscriptionAuthMode) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setSubscriptionAuthModeAsync(String s, SubscriptionAuthMode subscriptionAuthMode)
    {
        return null;
    }

    @Override
    public Integer getDeduplicationSnapshotInterval(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<Integer> getDeduplicationSnapshotIntervalAsync(String s)
    {
        return null;
    }

    @Override
    public void setDeduplicationSnapshotInterval(String s, Integer integer) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setDeduplicationSnapshotIntervalAsync(String s, Integer integer)
    {
        return null;
    }

    @Override
    public void removeDeduplicationSnapshotInterval(String s) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> removeDeduplicationSnapshotIntervalAsync(String s)
    {
        return null;
    }

    @Override
    public Integer getMaxSubscriptionsPerTopic(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<Integer> getMaxSubscriptionsPerTopicAsync(String s)
    {
        return null;
    }

    @Override
    public void setMaxSubscriptionsPerTopic(String s, int i) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setMaxSubscriptionsPerTopicAsync(String s, int i)
    {
        return null;
    }

    @Override
    public void removeMaxSubscriptionsPerTopic(String s) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> removeMaxSubscriptionsPerTopicAsync(String s)
    {
        return null;
    }

    @Override
    public Integer getMaxProducersPerTopic(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<Integer> getMaxProducersPerTopicAsync(String s)
    {
        return null;
    }

    @Override
    public void setMaxProducersPerTopic(String s, int i) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setMaxProducersPerTopicAsync(String s, int i)
    {
        return null;
    }

    @Override
    public void removeMaxProducersPerTopic(String s) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> removeMaxProducersPerTopicAsync(String s)
    {
        return null;
    }

    @Override
    public Integer getMaxConsumersPerTopic(String s) throws PulsarAdminException
    {
        return 0;
    }

    @Override
    public CompletableFuture<Integer> getMaxConsumersPerTopicAsync(String s)
    {
        return null;
    }

    @Override
    public void setMaxConsumersPerTopic(String s, int i) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setMaxConsumersPerTopicAsync(String s, int i)
    {
        return null;
    }

    @Override
    public void removeMaxConsumersPerTopic(String namespace) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> removeMaxConsumersPerTopicAsync(String namespace)
    {
        return null;
    }

    @Override
    public Integer getMaxConsumersPerSubscription(String s) throws PulsarAdminException
    {
        return 0;
    }

    @Override
    public CompletableFuture<Integer> getMaxConsumersPerSubscriptionAsync(String s)
    {
        return null;
    }

    @Override
    public void setMaxConsumersPerSubscription(String s, int i) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setMaxConsumersPerSubscriptionAsync(String s, int i)
    {
        return null;
    }

    @Override
    public void removeMaxConsumersPerSubscription(String namespace) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> removeMaxConsumersPerSubscriptionAsync(String namespace)
    {
        return null;
    }

    @Override
    public Integer getMaxUnackedMessagesPerConsumer(String s) throws PulsarAdminException
    {
        return 0;
    }

    @Override
    public CompletableFuture<Integer> getMaxUnackedMessagesPerConsumerAsync(String s)
    {
        return null;
    }

    @Override
    public void setMaxUnackedMessagesPerConsumer(String s, int i) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setMaxUnackedMessagesPerConsumerAsync(String s, int i)
    {
        return null;
    }

    @Override
    public void removeMaxUnackedMessagesPerConsumer(String namespace) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> removeMaxUnackedMessagesPerConsumerAsync(String namespace)
    {
        return null;
    }

    @Override
    public Integer getMaxUnackedMessagesPerSubscription(String s) throws PulsarAdminException
    {
        return 0;
    }

    @Override
    public CompletableFuture<Integer> getMaxUnackedMessagesPerSubscriptionAsync(String s)
    {
        return null;
    }

    @Override
    public void setMaxUnackedMessagesPerSubscription(String s, int i) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setMaxUnackedMessagesPerSubscriptionAsync(String s, int i)
    {
        return null;
    }

    @Override
    public void removeMaxUnackedMessagesPerSubscription(String namespace) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> removeMaxUnackedMessagesPerSubscriptionAsync(String namespace)
    {
        return null;
    }

    @Override
    public Long getCompactionThreshold(String s) throws PulsarAdminException
    {
        return 0L;
    }

    @Override
    public CompletableFuture<Long> getCompactionThresholdAsync(String s)
    {
        return null;
    }

    @Override
    public void setCompactionThreshold(String s, long l) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setCompactionThresholdAsync(String s, long l)
    {
        return null;
    }

    @Override
    public void removeCompactionThreshold(String namespace) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> removeCompactionThresholdAsync(String namespace)
    {
        return null;
    }

    @Override
    public long getOffloadThreshold(String s) throws PulsarAdminException
    {
        return 0;
    }

    @Override
    public CompletableFuture<Long> getOffloadThresholdAsync(String s)
    {
        return null;
    }

    @Override
    public void setOffloadThreshold(String s, long l) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setOffloadThresholdAsync(String s, long l)
    {
        return null;
    }

    @Override
    public Long getOffloadDeleteLagMs(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<Long> getOffloadDeleteLagMsAsync(String s)
    {
        return null;
    }

    @Override
    public void setOffloadDeleteLag(String s, long l, TimeUnit timeUnit) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setOffloadDeleteLagAsync(String s, long l, TimeUnit timeUnit)
    {
        return null;
    }

    @Override
    public void clearOffloadDeleteLag(String s) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> clearOffloadDeleteLagAsync(String s)
    {
        return null;
    }

    @Override
    public SchemaAutoUpdateCompatibilityStrategy getSchemaAutoUpdateCompatibilityStrategy(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public void setSchemaAutoUpdateCompatibilityStrategy(String s, SchemaAutoUpdateCompatibilityStrategy schemaAutoUpdateCompatibilityStrategy) throws PulsarAdminException
    { }

    @Override
    public boolean getSchemaValidationEnforced(String s) throws PulsarAdminException
    {
        return false;
    }

    @Override
    public CompletableFuture<Boolean> getSchemaValidationEnforcedAsync(String s)
    {
        return null;
    }

    @Override
    public void setSchemaValidationEnforced(String s, boolean b) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setSchemaValidationEnforcedAsync(String s, boolean b)
    {
        return null;
    }

    @Override
    public SchemaCompatibilityStrategy getSchemaCompatibilityStrategy(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<SchemaCompatibilityStrategy> getSchemaCompatibilityStrategyAsync(String s)
    {
        return null;
    }

    @Override
    public void setSchemaCompatibilityStrategy(String s, SchemaCompatibilityStrategy schemaCompatibilityStrategy) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setSchemaCompatibilityStrategyAsync(String s, SchemaCompatibilityStrategy schemaCompatibilityStrategy)
    {
        return null;
    }

    @Override
    public boolean getIsAllowAutoUpdateSchema(String s) throws PulsarAdminException
    {
        return false;
    }

    @Override
    public CompletableFuture<Boolean> getIsAllowAutoUpdateSchemaAsync(String s)
    {
        return null;
    }

    @Override
    public void setIsAllowAutoUpdateSchema(String s, boolean b) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setIsAllowAutoUpdateSchemaAsync(String s, boolean b)
    {
        return null;
    }

    @Override
    public void setOffloadPolicies(String s, OffloadPolicies offloadPolicies) throws PulsarAdminException
    { }

    @Override
    public void removeOffloadPolicies(String s) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setOffloadPoliciesAsync(String s, OffloadPolicies offloadPolicies)
    {
        return null;
    }

    @Override
    public CompletableFuture<Void> removeOffloadPoliciesAsync(String s)
    {
        return null;
    }

    @Override
    public OffloadPolicies getOffloadPolicies(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<OffloadPolicies> getOffloadPoliciesAsync(String s)
    {
        return null;
    }

    @Override
    public int getMaxTopicsPerNamespace(String namespace) throws PulsarAdminException
    {
        return 0;
    }

    @Override
    public CompletableFuture<Integer> getMaxTopicsPerNamespaceAsync(String namespace)
    {
        return null;
    }

    @Override
    public void setMaxTopicsPerNamespace(String namespace, int maxTopicsPerNamespace) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setMaxTopicsPerNamespaceAsync(String namespace, int maxTopicsPerNamespace)
    {
        return null;
    }

    @Override
    public void removeMaxTopicsPerNamespace(String namespace) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> removeMaxTopicsPerNamespaceAsync(String namespace)
    {
        return null;
    }

    @Override
    public CompletableFuture<Void> setPropertyAsync(String namespace, String key, String value)
    {
        return null;
    }

    @Override
    public void setProperty(String namespace, String key, String value) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setPropertiesAsync(String namespace, Map<String, String> properties)
    {
        return null;
    }

    @Override
    public void setProperties(String namespace, Map<String, String> properties) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<String> getPropertyAsync(String namespace, String key)
    {
        return null;
    }

    @Override
    public String getProperty(String namespace, String key) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<Map<String, String>> getPropertiesAsync(String namespace)
    {
        return null;
    }

    @Override
    public Map<String, String> getProperties(String namespace) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<String> removePropertyAsync(String namespace, String key)
    {
        return null;
    }

    @Override
    public String removeProperty(String namespace, String key) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<Void> clearPropertiesAsync(String namespace)
    {
        return null;
    }

    @Override
    public void clearProperties(String namespace) throws PulsarAdminException
    { }

    @Override
    public String getNamespaceResourceGroup(String namespace) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<String> getNamespaceResourceGroupAsync(String namespace)
    {
        return null;
    }

    @Override
    public void setNamespaceResourceGroup(String namespace, String resourcegroupname) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setNamespaceResourceGroupAsync(String namespace, String resourcegroupname)
    {
        return null;
    }

    @Override
    public void removeNamespaceResourceGroup(String namespace) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> removeNamespaceResourceGroupAsync(String namespace)
    {
        return null;
    }
}
