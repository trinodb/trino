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
package io.trino.plugin.pulsar;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.trino.plugin.pulsar.mock.MockPulsarSplitManager;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.testing.TestingConnectorSession;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.policies.data.OffloadedReadPriority;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.annotations.Test;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampType.TIMESTAMP;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestPulsarSplitManager
        extends TestPulsarConnector
{
    private static final Logger log = Logger.get(TestPulsarSplitManager.class);

    @Test(dataProvider = "rewriteNamespaceDelimiter", singleThreaded = true)
    public void testTopic(String delimiter) throws Exception
    {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
        List<TopicName> topics = new LinkedList<>();
        topics.addAll(topicNames.stream().filter(topicName -> !topicName.equals(NON_SCHEMA_TOPIC)).collect(Collectors.toList()));
        for (TopicName topicName : topics) {
            setup();
            log.info("!----- topic: %s -----!", topicName);
            PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(pulsarConnectorId.toString(),
                    topicName.getNamespace(),
                    topicName.getLocalName(),
                    topicName.getLocalName());
            PulsarTableLayoutHandle pulsarTableLayoutHandle = new PulsarTableLayoutHandle(pulsarTableHandle, TupleDomain.all());

            Collection<PulsarSplit> splits = new ArrayList<>();
            ((MockPulsarSplitManager) pulsarSplitManager).setSplits(splits);

            pulsarSplitManager.getSplits(
                    PulsarTransactionHandle.INSTANCE, TestingConnectorSession.SESSION,
                    pulsarTableLayoutHandle, null);

            int totalSize = 0;
            for (PulsarSplit pulsarSplit : splits) {
                assertEquals(pulsarSplit.getConnectorId(), pulsarConnectorId.toString());
                assertEquals(pulsarSplit.getSchemaName(), topicName.getNamespace());
                assertEquals(pulsarSplit.getTableName(), topicName.getLocalName());
                assertEquals(pulsarSplit.getSchema(),
                        new String(topicsToSchemas.get(topicName.getSchemaName()).getSchema(), StandardCharsets.ISO_8859_1));
                assertEquals(pulsarSplit.getSchemaType(), topicsToSchemas.get(topicName.getSchemaName()).getType());
                assertEquals(pulsarSplit.getStartPositionEntryId(), totalSize);
                assertEquals(pulsarSplit.getStartPositionLedgerId(), 0);
                assertEquals(pulsarSplit.getStartPosition(), PositionImpl.get(0, totalSize));
                assertEquals(pulsarSplit.getEndPositionLedgerId(), 0);
                assertEquals(pulsarSplit.getEndPositionEntryId(), totalSize + pulsarSplit.getSplitSize());
                assertEquals(pulsarSplit.getEndPosition(), PositionImpl.get(0, totalSize + pulsarSplit.getSplitSize()));

                totalSize += pulsarSplit.getSplitSize();
            }

            assertEquals(totalSize, topicsToNumEntries.get(topicName.getSchemaName()).intValue());
            cleanup();
        }
    }

    @Test(dataProvider = "rewriteNamespaceDelimiter", singleThreaded = true)
    public void testPartitionedTopic(String delimiter) throws Exception
    {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
        for (TopicName topicName : partitionedTopicNames) {
            setup();
            log.info("!----- topic: %s -----!", topicName);
            PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(pulsarConnectorId.toString(),
                    topicName.getNamespace(),
                    topicName.getLocalName(),
                    topicName.getLocalName());
            PulsarTableLayoutHandle pulsarTableLayoutHandle = new PulsarTableLayoutHandle(pulsarTableHandle, TupleDomain.all());

            Collection<PulsarSplit> splits = new ArrayList<>();
            ((MockPulsarSplitManager) pulsarSplitManager).setSplits(splits);

            pulsarSplitManager.getSplits(PulsarTransactionHandle.INSTANCE, TestingConnectorSession.SESSION,
                    pulsarTableLayoutHandle, null);

            int partitions = partitionedTopicsToPartitions.get(topicName.toString());

            for (int i = 0; i < partitions; i++) {
                List<PulsarSplit> splitsForPartition = getSplitsForPartition(topicName.getPartition(i), splits);
                int totalSize = 0;
                for (PulsarSplit pulsarSplit : splitsForPartition) {
                    assertEquals(pulsarSplit.getConnectorId(), pulsarConnectorId.toString());
                    assertEquals(pulsarSplit.getSchemaName(), topicName.getNamespace());
                    assertEquals(pulsarSplit.getTableName(), topicName.getPartition(i).getLocalName());
                    assertEquals(pulsarSplit.getSchema(),
                            new String(topicsToSchemas.get(topicName.getSchemaName()).getSchema(), StandardCharsets.ISO_8859_1));
                    assertEquals(pulsarSplit.getSchemaType(), topicsToSchemas.get(topicName.getSchemaName()).getType());
                    assertEquals(pulsarSplit.getStartPositionEntryId(), totalSize);
                    assertEquals(pulsarSplit.getStartPositionLedgerId(), 0);
                    assertEquals(pulsarSplit.getStartPosition(), PositionImpl.get(0, totalSize));
                    assertEquals(pulsarSplit.getEndPositionLedgerId(), 0);
                    assertEquals(pulsarSplit.getEndPositionEntryId(), totalSize + pulsarSplit.getSplitSize());
                    assertEquals(pulsarSplit.getEndPosition(), PositionImpl.get(0, totalSize + pulsarSplit.getSplitSize()));

                    totalSize += pulsarSplit.getSplitSize();
                }

                assertEquals(totalSize, topicsToNumEntries.get(topicName.getSchemaName()).intValue());
            }

            cleanup();
        }
    }

    private List<PulsarSplit> getSplitsForPartition(TopicName target, Collection<PulsarSplit> splits)
    {
        return splits.stream().filter(pulsarSplit -> {
            TopicName topicName = TopicName.get(pulsarSplit.getSchemaName() + "/" + pulsarSplit.getTableName());
            return target.equals(topicName);
        }).collect(Collectors.toList());
    }

    @Test(dataProvider = "rewriteNamespaceDelimiter", singleThreaded = true)
    public void testPublishTimePredicatePushdown(String delimiter) throws Exception
    {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
        TopicName topicName = TOPIC_1;

        setup();
        log.info("!----- topic: %s -----!", topicName);
        PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(pulsarConnectorId.toString(),
                topicName.getNamespace(),
                topicName.getLocalName(),
                topicName.getLocalName());

        Map<ColumnHandle, Domain> domainMap = new HashMap<>();
        Domain domain = Domain.create(ValueSet.ofRanges(Range.range(TIMESTAMP, currentTimeMs + 1L, true,
                currentTimeMs + 50L, true)), false);
        domainMap.put(PulsarInternalColumn.PUBLISH_TIME.getColumnHandle(pulsarConnectorId.toString(), false), domain);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(domainMap);

        PulsarTableLayoutHandle pulsarTableLayoutHandle = new PulsarTableLayoutHandle(pulsarTableHandle, tupleDomain);

        Collection<PulsarSplit> splits = new ArrayList<>();
        ((MockPulsarSplitManager) pulsarSplitManager).setSplits(splits);

        pulsarSplitManager.getSplits(
                PulsarTransactionHandle.INSTANCE, TestingConnectorSession.SESSION,
                pulsarTableLayoutHandle, null);

        int totalSize = 0;
        int initalStart = 1;
        for (PulsarSplit pulsarSplit : splits) {
            assertEquals(pulsarSplit.getConnectorId(), pulsarConnectorId.toString());
            assertEquals(pulsarSplit.getSchemaName(), topicName.getNamespace());
            assertEquals(pulsarSplit.getTableName(), topicName.getLocalName());
            assertEquals(pulsarSplit.getSchema(),
                    new String(topicsToSchemas.get(topicName.getSchemaName()).getSchema(), StandardCharsets.ISO_8859_1));
            assertEquals(pulsarSplit.getSchemaType(), topicsToSchemas.get(topicName.getSchemaName()).getType());
            assertEquals(pulsarSplit.getStartPositionEntryId(), initalStart);
            assertEquals(pulsarSplit.getStartPositionLedgerId(), 0);
            assertEquals(pulsarSplit.getStartPosition(), PositionImpl.get(0, initalStart));
            assertEquals(pulsarSplit.getEndPositionLedgerId(), 0);
            assertEquals(pulsarSplit.getEndPositionEntryId(), initalStart + pulsarSplit.getSplitSize());
            assertEquals(pulsarSplit.getEndPosition(), PositionImpl.get(0, initalStart + pulsarSplit
                    .getSplitSize()));

            initalStart += pulsarSplit.getSplitSize();
            totalSize += pulsarSplit.getSplitSize();
        }
        assertEquals(totalSize, 49);
    }

    @Test(dataProvider = "rewriteNamespaceDelimiter", singleThreaded = true)
    public void testPublishTimePredicatePushdownPartitionedTopic(String delimiter) throws Exception
    {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
        TopicName topicName = PARTITIONED_TOPIC_1;

        setup();
        log.info("!----- topic: %s -----!", topicName);
        PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(pulsarConnectorId.toString(),
                topicName.getNamespace(),
                topicName.getLocalName(),
                topicName.getLocalName());

        Map<ColumnHandle, Domain> domainMap = new HashMap<>();
        Domain domain = Domain.create(ValueSet.ofRanges(Range.range(TIMESTAMP, currentTimeMs + 1L, true,
                currentTimeMs + 50L, true)), false);
        domainMap.put(PulsarInternalColumn.PUBLISH_TIME.getColumnHandle(pulsarConnectorId.toString(), false), domain);
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(domainMap);

        PulsarTableLayoutHandle pulsarTableLayoutHandle = new PulsarTableLayoutHandle(pulsarTableHandle, tupleDomain);

        Collection<PulsarSplit> splits = new ArrayList<>();
        ((MockPulsarSplitManager) pulsarSplitManager).setSplits(splits);

        pulsarSplitManager.getSplits(
                PulsarTransactionHandle.INSTANCE, TestingConnectorSession.SESSION,
                pulsarTableLayoutHandle, null);

        int partitions = partitionedTopicsToPartitions.get(topicName.toString());
        for (int i = 0; i < partitions; i++) {
            List<PulsarSplit> splitsForPartition = getSplitsForPartition(topicName.getPartition(i), splits);
            int totalSize = 0;
            int initialStart = 1;
            for (PulsarSplit pulsarSplit : splitsForPartition) {
                assertEquals(pulsarSplit.getConnectorId(), pulsarConnectorId.toString());
                assertEquals(pulsarSplit.getSchemaName(), topicName.getNamespace());
                assertEquals(pulsarSplit.getTableName(), topicName.getPartition(i).getLocalName());
                assertEquals(pulsarSplit.getSchema(),
                        new String(topicsToSchemas.get(topicName.getSchemaName()).getSchema(), StandardCharsets.ISO_8859_1));
                assertEquals(pulsarSplit.getSchemaType(), topicsToSchemas.get(topicName.getSchemaName()).getType());
                assertEquals(pulsarSplit.getStartPositionEntryId(), initialStart);
                assertEquals(pulsarSplit.getStartPositionLedgerId(), 0);
                assertEquals(pulsarSplit.getStartPosition(), PositionImpl.get(0, initialStart));
                assertEquals(pulsarSplit.getEndPositionLedgerId(), 0);
                assertEquals(pulsarSplit.getEndPositionEntryId(), initialStart + pulsarSplit.getSplitSize());
                assertEquals(pulsarSplit.getEndPosition(), PositionImpl.get(0, initialStart + pulsarSplit.getSplitSize()));

                initialStart += pulsarSplit.getSplitSize();
                totalSize += pulsarSplit.getSplitSize();
            }

            assertEquals(totalSize, 49);
        }
    }

    @Test(dataProvider = "rewriteNamespaceDelimiter", singleThreaded = true)
    public void testPartitionFilter(String delimiter) throws Exception
    {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
        for (TopicName topicName : partitionedTopicNames) {
            setup();
            log.info("!----- topic: %s -----!", topicName);
            PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(pulsarConnectorId.toString(), topicName.getNamespace(),
                    topicName.getLocalName(), topicName.getLocalName());

            // test single domain with equal low and high of "__partition__"
            Map<ColumnHandle, Domain> domainMap = new HashMap<>();
            Domain domain = Domain.create(ValueSet.ofRanges(Range.range(INTEGER, 0L, true,
                    0L, true)), false);
            domainMap.put(PulsarInternalColumn.PARTITION.getColumnHandle(pulsarConnectorId.toString(), false), domain);
            TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(domainMap);

            ((MockPulsarSplitManager) pulsarSplitManager).setSplits(new ArrayList<>());

            Collection<PulsarSplit> splits = pulsarSplitManager.getSplitsPartitionedTopic(2, topicName, pulsarTableHandle,
                    schemas.getSchemaInfo(topicName.getSchemaName()), tupleDomain, null);
            if (topicsToNumEntries.get(topicName.getSchemaName()) > 1) {
                assertEquals(splits.size(), 2);
            }
            for (PulsarSplit split : splits) {
                assertEquals(TopicName.getPartitionIndex(split.getTableName()), 0);
            }

            // test multiple domain with equal low and high of "__partition__"
            domainMap.clear();
            domain = Domain.create(ValueSet.ofRanges(
                    Range.range(INTEGER, 0L, true, 0L, true),
                    Range.range(INTEGER, 3L, true, 3L, true)),
                    false);
            domainMap.put(PulsarInternalColumn.PARTITION.getColumnHandle(pulsarConnectorId.toString(), false), domain);
            tupleDomain = TupleDomain.withColumnDomains(domainMap);
            splits = pulsarSplitManager.getSplitsPartitionedTopic(1, topicName, pulsarTableHandle,
                    schemas.getSchemaInfo(topicName.getSchemaName()), tupleDomain, null);
            if (topicsToNumEntries.get(topicName.getSchemaName()) > 1) {
                assertEquals(splits.size(), 2);
            }
            for (PulsarSplit split : splits) {
                assertTrue(TopicName.getPartitionIndex(split.getTableName()) == 0 || TopicName.getPartitionIndex(split.getTableName()) == 3);
            }

            // test single domain with unequal low and high of "__partition__"
            domainMap.clear();
            domain = Domain.create(ValueSet.ofRanges(
                    Range.range(INTEGER, 0L, true, 2L, true)),
                    false);
            domainMap.put(PulsarInternalColumn.PARTITION.getColumnHandle(pulsarConnectorId.toString(), false), domain);
            tupleDomain = TupleDomain.withColumnDomains(domainMap);
            splits = this.pulsarSplitManager.getSplitsPartitionedTopic(2, topicName, pulsarTableHandle,
                    schemas.getSchemaInfo(topicName.getSchemaName()), tupleDomain, null);
            if (topicsToNumEntries.get(topicName.getSchemaName()) > 1) {
                assertEquals(splits.size(), 3);
            }
            for (PulsarSplit split : splits) {
                assertTrue(TopicName.getPartitionIndex(split.getTableName()) == 0
                        || TopicName.getPartitionIndex(split.getTableName()) == 1
                        || TopicName.getPartitionIndex(split.getTableName()) == 2);
            }

            // test multiple domain with unequal low and high of "__partition__"
            domainMap.clear();
            domain = Domain.create(ValueSet.ofRanges(
                    Range.range(INTEGER, 0L, true, 1L, true),
                    Range.range(INTEGER, 3L, true, 4L, true)),
                    false);
            domainMap.put(PulsarInternalColumn.PARTITION.getColumnHandle(pulsarConnectorId.toString(), false), domain);
            tupleDomain = TupleDomain.withColumnDomains(domainMap);
            splits = this.pulsarSplitManager.getSplitsPartitionedTopic(2, topicName, pulsarTableHandle,
                    schemas.getSchemaInfo(topicName.getSchemaName()), tupleDomain, null);
            if (topicsToNumEntries.get(topicName.getSchemaName()) > 1) {
                assertEquals(splits.size(), 4);
            }
            for (PulsarSplit split : splits) {
                assertTrue(TopicName.getPartitionIndex(split.getTableName()) == 0
                        || TopicName.getPartitionIndex(split.getTableName()) == 1
                        || TopicName.getPartitionIndex(split.getTableName()) == 3
                        || TopicName.getPartitionIndex(split.getTableName()) == 4);
            }
        }
    }

    @Test(dataProvider = "rewriteNamespaceDelimiter", singleThreaded = true)
    public void testGetSplitNonSchema(String delimiter) throws Exception
    {
        updateRewriteNamespaceDelimiterIfNeeded(delimiter);
        TopicName topicName = NON_SCHEMA_TOPIC;
        setup();
        log.info("!----- topic: %s -----!", topicName);
        PulsarTableHandle pulsarTableHandle = new PulsarTableHandle(pulsarConnectorId.toString(),
                topicName.getNamespace(),
                topicName.getLocalName(),
                topicName.getLocalName());

        Map<ColumnHandle, Domain> domainMap = new HashMap<>();
        TupleDomain<ColumnHandle> tupleDomain = TupleDomain.withColumnDomains(domainMap);

        PulsarTableLayoutHandle pulsarTableLayoutHandle = new PulsarTableLayoutHandle(pulsarTableHandle, tupleDomain);
        ((MockPulsarSplitManager) this.pulsarSplitManager).setSplits(new ArrayList<>());
        ConnectorSplitSource connectorSplitSource = this.pulsarSplitManager.getSplits(
                PulsarTransactionHandle.INSTANCE, TestingConnectorSession.SESSION,
                pulsarTableLayoutHandle, null);
        assertNotNull(connectorSplitSource);
    }

    @Test
    public void pulsarSplitJsonCodecTest() throws JsonProcessingException, UnsupportedEncodingException, JsonProcessingException
    {
        OffloadPoliciesImpl offloadPolicies = OffloadPoliciesImpl.create(
                "aws-s3",
                "test-region",
                "test-bucket",
                "test-endpoint",
                "role-",
                "role-session-name",
                "test-credential-id",
                "test-credential-secret",
                5000,
                2000,
                1000L,
                5000L,
                OffloadedReadPriority.TIERED_STORAGE_FIRST);

        SchemaInfo schemaInfo = JSONSchema.of(Foo.class).getSchemaInfo();
        final String schema = new String(schemaInfo.getSchema(), StandardCharsets.ISO_8859_1);
        final String originSchemaName = schemaInfo.getName();
        final String schemaName = schemaInfo.getName();
        final String schemaInfoProperties = new ObjectMapper().writeValueAsString(schemaInfo.getProperties());
        final SchemaType schemaType = schemaInfo.getType();

        final long splitId = 1;
        final String connectorId = "connectorId";
        final String tableName = "tableName";
        final long splitSize = 5;
        final long startPositionEntryId = 22;
        final long endPositionEntryId = 33;
        final long startPositionLedgerId = 10;
        final long endPositionLedgerId = 21;
        final TupleDomain<ColumnHandle> tupleDomain = TupleDomain.all();

        byte[] pulsarSplitData;
        JsonCodec<PulsarSplit> jsonCodec = JsonCodec.jsonCodec(PulsarSplit.class);
        try {
            PulsarSplit pulsarSplit = new PulsarSplit(
                    splitId, connectorId, schemaName, originSchemaName, tableName, splitSize, schema,
                    schemaType, startPositionEntryId, endPositionEntryId, startPositionLedgerId,
                    endPositionLedgerId, tupleDomain, schemaInfoProperties, offloadPolicies);
            pulsarSplitData = jsonCodec.toJsonBytes(pulsarSplit);
        }
        catch (Exception e) {
            e.printStackTrace();
            log.error("Failed to serialize the PulsarSplit.", e);
            fail("Failed to serialize the PulsarSplit.");
            return;
        }

        try {
            PulsarSplit pulsarSplit = jsonCodec.fromJson(pulsarSplitData);
            assertEquals(pulsarSplit.getSchema(), schema);
            assertEquals(pulsarSplit.getOriginSchemaName(), originSchemaName);
            assertEquals(pulsarSplit.getSchemaName(), schemaName);
            assertEquals(pulsarSplit.getSchemaInfoProperties(), schemaInfoProperties);
            assertEquals(pulsarSplit.getSchemaType(), schemaType);
            assertEquals(pulsarSplit.getSplitId(), splitId);
            assertEquals(pulsarSplit.getConnectorId(), connectorId);
            assertEquals(pulsarSplit.getTableName(), tableName);
            assertEquals(pulsarSplit.getSplitSize(), splitSize);
            assertEquals(pulsarSplit.getStartPositionEntryId(), startPositionEntryId);
            assertEquals(pulsarSplit.getEndPositionEntryId(), endPositionEntryId);
            assertEquals(pulsarSplit.getStartPositionLedgerId(), startPositionLedgerId);
            assertEquals(pulsarSplit.getEndPositionLedgerId(), endPositionLedgerId);
            assertEquals(pulsarSplit.getTupleDomain(), tupleDomain);
            assertEquals(pulsarSplit.getOffloadPolicies(), offloadPolicies);
        }
        catch (Exception e) {
            log.error("Failed to deserialize the PulsarSplit.", e);
            fail("Failed to deserialize the PulsarSplit.");
        }
    }
}
