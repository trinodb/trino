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

import org.apache.pulsar.client.admin.LongRunningProcessStatus;
import org.apache.pulsar.client.admin.OffloadProcessStatus;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.apache.pulsar.common.policies.data.PartitionedTopicInternalStats;
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SubscribeRate;
import org.apache.pulsar.common.policies.data.TopicStats;

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.core.Response;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class MockTopics
        implements Topics
{
    private List<TopicName> topicNames;
    private List<TopicName> partitionedTopicNames;
    private Map<String, Integer> partitionedTopicsToPartitions;

    public MockTopics(List<TopicName> topicNames, List<TopicName> partitionedTopicNames,
                      Map<String, Integer> partitionedTopicsToPartitions)
    {
        this.topicNames = topicNames;
        this.partitionedTopicNames = partitionedTopicNames;
        this.partitionedTopicsToPartitions = partitionedTopicsToPartitions;
    }

    @Override
    public List<String> getList(String namespace) throws PulsarAdminException
    {
        List<String> topics = new ArrayList<>(topicNames.stream()
                .filter(topicName -> topicName.getNamespace().equals(namespace))
                .map(TopicName::toString).collect(Collectors.toList()));
        partitionedTopicNames.stream().filter(topicName -> topicName.getNamespace().equals(namespace)).forEach(topicName -> {
            for (Integer i = 0; i < partitionedTopicsToPartitions.get(topicName.toString()); i++) {
                topics.add(TopicName.get(topicName + "-partition-" + i).toString());
            }
        });
        if (topics.isEmpty()) {
            throw new PulsarAdminException(new ClientErrorException(Response.status(404).build()));
        }
        return topics;
    }

    @Override
    public List<String> getList(String namespace, TopicDomain topicDomain) throws PulsarAdminException
    {
        List<String> topics = new ArrayList<>(topicNames.stream()
                .filter(topicName -> topicName.getNamespace().equals(namespace))
                .map(TopicName::toString).collect(Collectors.toList()));
        partitionedTopicNames.stream().filter(topicName -> topicName.getNamespace().equals(namespace)).forEach(topicName -> {
            for (Integer i = 0; i < partitionedTopicsToPartitions.get(topicName.toString()); i++) {
                topics.add(TopicName.get(topicName + "-partition-" + i).toString());
            }
        });
        if (topics.isEmpty()) {
            ClientErrorException cee = new ClientErrorException(Response.Status.NOT_FOUND);
            throw new PulsarAdminException(cee, cee.getMessage(), cee.getResponse().getStatus());
        }
        return topics;
    }

    @Override
    public List<String> getPartitionedTopicList(String ns) throws PulsarAdminException
    {
        List<String> topics = partitionedTopicNames.stream()
                .filter(topicName -> topicName.getNamespace().equals(ns))
                .map(TopicName::toString)
                .collect(Collectors.toList());
        if (topics.isEmpty()) {
            throw new PulsarAdminException(new ClientErrorException(Response.status(404).build()));
        }
        return topics;
    }

    @Override
    public PartitionedTopicMetadata getPartitionedTopicMetadata(String topic) throws PulsarAdminException
    {
        int partitions = partitionedTopicsToPartitions.get(topic) == null
                ? 0 : partitionedTopicsToPartitions.get(topic);
        return new PartitionedTopicMetadata(partitions);
    }

    @Override
    public CompletableFuture<List<String>> getListAsync(String s)
    {
        return null;
    }

    @Override
    public CompletableFuture<List<String>> getListAsync(String namespace, TopicDomain topicDomain)
    {
        return null;
    }

    @Override
    public CompletableFuture<List<String>> getPartitionedTopicListAsync(String s)
    {
        return null;
    }

    @Override
    public List<String> getListInBundle(String s, String s1) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<List<String>> getListInBundleAsync(String s, String s1)
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
    public void grantPermission(String s, String s1, Set<AuthAction> set) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> grantPermissionAsync(String s, String s1, Set<AuthAction> set)
    {
        return null;
    }

    @Override
    public void revokePermissions(String s, String s1) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> revokePermissionsAsync(String s, String s1)
    {
        return null;
    }

    @Override
    public void createPartitionedTopic(String s, int i) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> createPartitionedTopicAsync(String s, int i)
    {
        return null;
    }

    @Override
    public void createNonPartitionedTopic(String s) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> createNonPartitionedTopicAsync(String s)
    {
        return null;
    }

    @Override
    public void createMissedPartitions(String s) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> createMissedPartitionsAsync(String s)
    {
        return null;
    }

    @Override
    public void updatePartitionedTopic(String s, int i) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> updatePartitionedTopicAsync(String s, int i)
    {
        return null;
    }

    @Override
    public void updatePartitionedTopic(String s, int i, boolean b) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> updatePartitionedTopicAsync(String s, int i, boolean b)
    {
        return null;
    }

    @Override
    public CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadataAsync(String s)
    {
        return null;
    }

    @Override
    public void deletePartitionedTopic(String s, boolean b, boolean b1) throws PulsarAdminException
    { }

    @Override
    public void deletePartitionedTopic(String topic, boolean force) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> deletePartitionedTopicAsync(String s, boolean b, boolean b1)
    {
        return null;
    }

    @Override
    public CompletableFuture<Void> deletePartitionedTopicAsync(String topic, boolean force)
    {
        return null;
    }

    @Override
    public void deletePartitionedTopic(String s) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> deletePartitionedTopicAsync(String s)
    {
        return null;
    }

    @Override
    public void delete(String s, boolean b, boolean b1) throws PulsarAdminException
    { }

    @Override
    public void delete(String topic, boolean force) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> deleteAsync(String s, boolean b, boolean b1)
    {
        return null;
    }

    @Override
    public CompletableFuture<Void> deleteAsync(String topic, boolean force)
    {
        return null;
    }

    @Override
    public void delete(String s) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> deleteAsync(String s)
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
    public MessageId terminateTopic(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<MessageId> terminateTopicAsync(String s)
    {
        return null;
    }

    @Override
    public List<String> getSubscriptions(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<List<String>> getSubscriptionsAsync(String s)
    {
        return null;
    }

    @Override
    public TopicStats getStats(String s, boolean b, boolean b1) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public TopicStats getStats(String topic, boolean getPreciseBacklog) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public TopicStats getStats(String topic) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<TopicStats> getStatsAsync(String s, boolean b, boolean b1)
    {
        return null;
    }

    @Override
    public CompletableFuture<TopicStats> getStatsAsync(String topic)
    {
        return null;
    }

    @Override
    public PersistentTopicInternalStats getInternalStats(String s, boolean b) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public PersistentTopicInternalStats getInternalStats(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<PersistentTopicInternalStats> getInternalStatsAsync(String s, boolean b)
    {
        return null;
    }

    @Override
    public CompletableFuture<PersistentTopicInternalStats> getInternalStatsAsync(String s)
    {
        return null;
    }

    @Override
    public String getInternalInfo(String topic) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<String> getInternalInfoAsync(String topic)
    {
        return null;
    }

    @Override
    public PartitionedTopicStats getPartitionedStats(String s, boolean b, boolean b1, boolean b2) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public PartitionedTopicStats getPartitionedStats(String topic, boolean perPartition) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<PartitionedTopicStats> getPartitionedStatsAsync(String s, boolean b, boolean b1, boolean b2)
    {
        return null;
    }

    @Override
    public CompletableFuture<PartitionedTopicStats> getPartitionedStatsAsync(String topic, boolean perPartition)
    {
        return null;
    }

    @Override
    public PartitionedTopicInternalStats getPartitionedInternalStats(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<PartitionedTopicInternalStats> getPartitionedInternalStatsAsync(String s)
    {
        return null;
    }

    @Override
    public void deleteSubscription(String s, String s1) throws PulsarAdminException
    { }

    @Override
    public void deleteSubscription(String s, String s1, boolean b) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> deleteSubscriptionAsync(String s, String s1)
    {
        return null;
    }

    @Override
    public CompletableFuture<Void> deleteSubscriptionAsync(String s, String s1, boolean b)
    {
        return null;
    }

    @Override
    public void skipAllMessages(String s, String s1) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> skipAllMessagesAsync(String s, String s1)
    {
        return null;
    }

    @Override
    public void skipMessages(String s, String s1, long l) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> skipMessagesAsync(String s, String s1, long l)
    {
        return null;
    }

    @Override
    public void expireMessages(String s, String s1, long l) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> expireMessagesAsync(String s, String s1, long l)
    {
        return null;
    }

    @Override
    public void expireMessages(String s, String s1, MessageId messageId, boolean b) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> expireMessagesAsync(String s, String s1, MessageId messageId, boolean b)
    {
        return null;
    }

    @Override
    public void expireMessagesForAllSubscriptions(String s, long l) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> expireMessagesForAllSubscriptionsAsync(String s, long l)
    {
        return null;
    }

    @Override
    public List<Message<byte[]>> peekMessages(String s, String s1, int i) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<List<Message<byte[]>>> peekMessagesAsync(String s, String s1, int i)
    {
        return null;
    }

    @Override
    public Message<byte[]> getMessageById(String s, long l, long l1) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<Message<byte[]>> getMessageByIdAsync(String s, long l, long l1)
    {
        return null;
    }

    @Override
    public void createSubscription(String s, String s1, MessageId messageId) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> createSubscriptionAsync(String s, String s1, MessageId messageId)
    {
        return null;
    }

    @Override
    public void resetCursor(String s, String s1, long l) throws PulsarAdminException
    { }

    @Override
    public void resetCursor(String s, String s1, MessageId messageId, boolean b) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> resetCursorAsync(String s, String s1, long l)
    {
        return null;
    }

    @Override
    public CompletableFuture<Void> resetCursorAsync(String s, String s1, MessageId messageId, boolean b)
    {
        return null;
    }

    @Override
    public void resetCursor(String s, String s1, MessageId messageId) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> resetCursorAsync(String s, String s1, MessageId messageId)
    {
        return null;
    }

    @Override
    public void triggerCompaction(String s) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> triggerCompactionAsync(String s)
    {
        return null;
    }

    @Override
    public LongRunningProcessStatus compactionStatus(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<LongRunningProcessStatus> compactionStatusAsync(String s)
    {
        return null;
    }

    @Override
    public void triggerOffload(String s, MessageId messageId) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> triggerOffloadAsync(String s, MessageId messageId)
    {
        return null;
    }

    @Override
    public OffloadProcessStatus offloadStatus(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<OffloadProcessStatus> offloadStatusAsync(String s)
    {
        return null;
    }

    @Override
    public MessageId getLastMessageId(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<MessageId> getLastMessageIdAsync(String s)
    {
        return null;
    }

    @Override
    public Map<BacklogQuota.BacklogQuotaType, BacklogQuota> getBacklogQuotaMap(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public Map<BacklogQuota.BacklogQuotaType, BacklogQuota> getBacklogQuotaMap(String topic, boolean applied) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public void setBacklogQuota(String s, BacklogQuota backlogQuota) throws PulsarAdminException
    { }

    @Override
    public void removeBacklogQuota(String s) throws PulsarAdminException
    { }

    @Override
    public DelayedDeliveryPolicies getDelayedDeliveryPolicy(String topic, boolean applied) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<DelayedDeliveryPolicies> getDelayedDeliveryPolicyAsync(String topic, boolean applied)
    {
        return null;
    }

    @Override
    public DelayedDeliveryPolicies getDelayedDeliveryPolicy(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<DelayedDeliveryPolicies> getDelayedDeliveryPolicyAsync(String s)
    {
        return null;
    }

    @Override
    public void setDelayedDeliveryPolicy(String s, DelayedDeliveryPolicies delayedDeliveryPolicies) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setDelayedDeliveryPolicyAsync(String s, DelayedDeliveryPolicies delayedDeliveryPolicies)
    {
        return null;
    }

    @Override
    public CompletableFuture<Void> removeDelayedDeliveryPolicyAsync(String s)
    {
        return null;
    }

    @Override
    public void removeDelayedDeliveryPolicy(String s) throws PulsarAdminException
    { }

    @Override
    public void setMessageTTL(String s, int i) throws PulsarAdminException
    { }

    @Override
    public Integer getMessageTTL(String s) throws PulsarAdminException
    {
        return 0;
    }

    @Override
    public Integer getMessageTTL(String topic, boolean applied) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public void removeMessageTTL(String s) throws PulsarAdminException
    { }

    @Override
    public void setRetention(String s, RetentionPolicies retentionPolicies) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setRetentionAsync(String s, RetentionPolicies retentionPolicies)
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
    public RetentionPolicies getRetention(String topic, boolean applied) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<RetentionPolicies> getRetentionAsync(String topic, boolean applied)
    {
        return null;
    }

    @Override
    public void removeRetention(String s) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> removeRetentionAsync(String s)
    {
        return null;
    }

    @Override
    public Integer getMaxUnackedMessagesOnConsumer(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<Integer> getMaxUnackedMessagesOnConsumerAsync(String s)
    {
        return null;
    }

    @Override
    public Integer getMaxUnackedMessagesOnConsumer(String topic, boolean applied) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<Integer> getMaxUnackedMessagesOnConsumerAsync(String topic, boolean applied)
    {
        return null;
    }

    @Override
    public void setMaxUnackedMessagesOnConsumer(String s, int i) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setMaxUnackedMessagesOnConsumerAsync(String s, int i)
    {
        return null;
    }

    @Override
    public void removeMaxUnackedMessagesOnConsumer(String s) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> removeMaxUnackedMessagesOnConsumerAsync(String s)
    {
        return null;
    }

    @Override
    public InactiveTopicPolicies getInactiveTopicPolicies(String topic, boolean applied) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<InactiveTopicPolicies> getInactiveTopicPoliciesAsync(String topic, boolean applied)
    {
        return null;
    }

    @Override
    public InactiveTopicPolicies getInactiveTopicPolicies(String s) throws PulsarAdminException
    {
        return null;
    }

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
    public void removeInactiveTopicPolicies(String s) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> removeInactiveTopicPoliciesAsync(String s)
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
    public OffloadPolicies getOffloadPolicies(String topic, boolean applied) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<OffloadPolicies> getOffloadPoliciesAsync(String topic, boolean applied)
    {
        return null;
    }

    @Override
    public void setOffloadPolicies(String s, OffloadPolicies offloadPolicies) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setOffloadPoliciesAsync(String s, OffloadPolicies offloadPolicies)
    {
        return null;
    }

    @Override
    public void removeOffloadPolicies(String s) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> removeOffloadPoliciesAsync(String s)
    {
        return null;
    }

    @Override
    public Integer getMaxUnackedMessagesOnSubscription(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<Integer> getMaxUnackedMessagesOnSubscriptionAsync(String s)
    {
        return null;
    }

    @Override
    public Integer getMaxUnackedMessagesOnSubscription(String topic, boolean applied) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<Integer> getMaxUnackedMessagesOnSubscriptionAsync(String topic, boolean applied)
    {
        return null;
    }

    @Override
    public void setMaxUnackedMessagesOnSubscription(String s, int i) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setMaxUnackedMessagesOnSubscriptionAsync(String s, int i)
    {
        return null;
    }

    @Override
    public void removeMaxUnackedMessagesOnSubscription(String s) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> removeMaxUnackedMessagesOnSubscriptionAsync(String s)
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
    public PersistencePolicies getPersistence(String topic, boolean applied) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<PersistencePolicies> getPersistenceAsync(String topic, boolean applied)
    {
        return null;
    }

    @Override
    public void removePersistence(String s) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> removePersistenceAsync(String s)
    {
        return null;
    }

    @Override
    public Boolean getDeduplicationEnabled(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<Boolean> getDeduplicationEnabledAsync(String s)
    {
        return null;
    }

    @Override
    public Boolean getDeduplicationStatus(String topic) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<Boolean> getDeduplicationStatusAsync(String topic)
    {
        return null;
    }

    @Override
    public Boolean getDeduplicationStatus(String topic, boolean applied) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<Boolean> getDeduplicationStatusAsync(String topic, boolean applied)
    {
        return null;
    }

    @Override
    public void enableDeduplication(String s, boolean b) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> enableDeduplicationAsync(String s, boolean b)
    {
        return null;
    }

    @Override
    public void setDeduplicationStatus(String topic, boolean enabled) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setDeduplicationStatusAsync(String topic, boolean enabled)
    {
        return null;
    }

    @Override
    public void disableDeduplication(String s) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> disableDeduplicationAsync(String s)
    {
        return null;
    }

    @Override
    public void removeDeduplicationStatus(String topic) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> removeDeduplicationStatusAsync(String topic)
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
    public DispatchRate getDispatchRate(String topic, boolean applied) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<DispatchRate> getDispatchRateAsync(String topic, boolean applied)
    {
        return null;
    }

    @Override
    public void removeDispatchRate(String s) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> removeDispatchRateAsync(String s) throws PulsarAdminException
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
    public DispatchRate getSubscriptionDispatchRate(String namespace, boolean applied) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<DispatchRate> getSubscriptionDispatchRateAsync(String namespace, boolean applied)
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
    public void removeSubscriptionDispatchRate(String s) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> removeSubscriptionDispatchRateAsync(String s)
    {
        return null;
    }

    @Override
    public void setReplicatorDispatchRate(String topic, DispatchRate dispatchRate) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setReplicatorDispatchRateAsync(String topic, DispatchRate dispatchRate)
    {
        return null;
    }

    @Override
    public DispatchRate getReplicatorDispatchRate(String topic) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<DispatchRate> getReplicatorDispatchRateAsync(String topic)
    {
        return null;
    }

    @Override
    public DispatchRate getReplicatorDispatchRate(String topic, boolean applied) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<DispatchRate> getReplicatorDispatchRateAsync(String topic, boolean applied)
    {
        return null;
    }

    @Override
    public void removeReplicatorDispatchRate(String topic) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> removeReplicatorDispatchRateAsync(String topic)
    {
        return null;
    }

    @Override
    public Long getCompactionThreshold(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<Long> getCompactionThresholdAsync(String s)
    {
        return null;
    }

    @Override
    public Long getCompactionThreshold(String topic, boolean applied) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<Long> getCompactionThresholdAsync(String topic, boolean applied)
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
    public void removeCompactionThreshold(String s) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> removeCompactionThresholdAsync(String s)
    {
        return null;
    }

    @Override
    public void setPublishRate(String s, PublishRate publishRate) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setPublishRateAsync(String s, PublishRate publishRate)
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
    public void removePublishRate(String s) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> removePublishRateAsync(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public Integer getMaxConsumersPerSubscription(String s) throws PulsarAdminException
    {
        return null;
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
    public void removeMaxConsumersPerSubscription(String s) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> removeMaxConsumersPerSubscriptionAsync(String s)
    {
        return null;
    }

    @Override
    public Integer getMaxProducers(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<Integer> getMaxProducersAsync(String s)
    {
        return null;
    }

    @Override
    public Integer getMaxProducers(String topic, boolean applied) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<Integer> getMaxProducersAsync(String topic, boolean applied)
    {
        return null;
    }

    @Override
    public void setMaxProducers(String s, int i) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setMaxProducersAsync(String s, int i)
    {
        return null;
    }

    @Override
    public void removeMaxProducers(String s) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> removeMaxProducersAsync(String s)
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
    public Integer getMaxMessageSize(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<Integer> getMaxMessageSizeAsync(String s)
    {
        return null;
    }

    @Override
    public void setMaxMessageSize(String s, int i) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setMaxMessageSizeAsync(String s, int i)
    {
        return null;
    }

    @Override
    public void removeMaxMessageSize(String s) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> removeMaxMessageSizeAsync(String s)
    {
        return null;
    }

    @Override
    public Integer getMaxConsumers(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<Integer> getMaxConsumersAsync(String s)
    {
        return null;
    }

    @Override
    public Integer getMaxConsumers(String topic, boolean applied) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<Integer> getMaxConsumersAsync(String topic, boolean applied)
    {
        return null;
    }

    @Override
    public void setMaxConsumers(String s, int i) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setMaxConsumersAsync(String s, int i)
    {
        return null;
    }

    @Override
    public void removeMaxConsumers(String s) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> removeMaxConsumersAsync(String s)
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
    public void setDeduplicationSnapshotInterval(String s, int i) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setDeduplicationSnapshotIntervalAsync(String s, int i)
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
    public void setSubscriptionTypesEnabled(String topic, Set<SubscriptionType> subscriptionTypesEnabled) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> setSubscriptionTypesEnabledAsync(String topic, Set<SubscriptionType> subscriptionTypesEnabled)
    {
        return null;
    }

    @Override
    public Set<SubscriptionType> getSubscriptionTypesEnabled(String topic) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<Set<SubscriptionType>> getSubscriptionTypesEnabledAsync(String topic)
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
    public SubscribeRate getSubscribeRate(String topic, boolean applied) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<SubscribeRate> getSubscribeRateAsync(String topic, boolean applied)
    {
        return null;
    }

    @Override
    public void removeSubscribeRate(String s) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> removeSubscribeRateAsync(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public Message<byte[]> examineMessage(String s, String s1, long l) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<Message<byte[]>> examineMessageAsync(String s, String s1, long l) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public void truncate(String topic) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> truncateAsync(String topic)
    {
        return null;
    }
}
