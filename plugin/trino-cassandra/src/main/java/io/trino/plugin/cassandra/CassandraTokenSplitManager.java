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
package io.trino.plugin.cassandra;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.TrinoException;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.cassandra.CassandraErrorCode.CASSANDRA_METADATA_ERROR;
import static io.trino.plugin.cassandra.TokenRing.createForPartitioner;
import static java.lang.Math.max;
import static java.lang.Math.round;
import static java.lang.StrictMath.toIntExact;
import static java.util.Collections.shuffle;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public class CassandraTokenSplitManager
{
    private final CassandraSession session;
    private final int splitSize;
    private final Optional<Long> configSplitsPerNode;

    @Inject
    public CassandraTokenSplitManager(CassandraSession session, CassandraClientConfig config)
    {
        this(session, config.getSplitSize(), config.getSplitsPerNode());
    }

    public CassandraTokenSplitManager(CassandraSession session, int splitSize, Optional<Long> configSplitsPerNode)
    {
        this.session = requireNonNull(session, "session is null");
        this.splitSize = splitSize;
        this.configSplitsPerNode = configSplitsPerNode;
    }

    public List<TokenSplit> getSplits(String keyspace, String table, Optional<Long> sessionSplitsPerNode)
    {
        Set<TokenRange> tokenRanges = session.getTokenRanges();

        if (tokenRanges.isEmpty()) {
            throw new TrinoException(CASSANDRA_METADATA_ERROR, "The cluster metadata is not available. " +
                    "Please make sure that the Cassandra cluster is up and running, " +
                    "and that the contact points are specified correctly.");
        }

        if (tokenRanges.stream().anyMatch(TokenRange::isWrappedAround)) {
            tokenRanges = unwrap(tokenRanges);
        }

        Optional<TokenRing> tokenRing = createForPartitioner(session.getPartitioner());
        long totalPartitionsCount = getTotalPartitionsCount(keyspace, table, sessionSplitsPerNode);

        List<TokenSplit> splits = new ArrayList<>();
        for (TokenRange tokenRange : tokenRanges) {
            if (tokenRange.isEmpty()) {
                continue;
            }
            checkState(!tokenRange.isWrappedAround(), "all token ranges must be unwrapped at this step");

            List<String> endpoints = getEndpoints(keyspace, tokenRange);
            checkState(!endpoints.isEmpty(), "endpoints is empty for token range: %s", tokenRange);

            if (tokenRing.isEmpty()) {
                checkState(!tokenRange.isWrappedAround(), "all token ranges must be unwrapped at this step");
                splits.add(createSplit(tokenRange, endpoints));
                continue;
            }

            double tokenRangeRingFraction = tokenRing.get().getRingFraction(tokenRange.getStart(), tokenRange.getEnd());
            long partitionsCountEstimate = round(totalPartitionsCount * tokenRangeRingFraction);
            checkState(partitionsCountEstimate >= 0, "unexpected partitions count estimate: %s", partitionsCountEstimate);
            int subSplitCount = max(toIntExact(partitionsCountEstimate / splitSize), 1);
            List<TokenRange> subRanges = tokenRange.splitEvenly(subSplitCount);

            for (TokenRange subRange : subRanges) {
                if (subRange.isEmpty()) {
                    continue;
                }
                checkState(!subRange.isWrappedAround(), "all token ranges must be unwrapped at this step");
                splits.add(createSplit(subRange, endpoints));
            }
        }
        shuffle(splits, ThreadLocalRandom.current());
        return unmodifiableList(splits);
    }

    private Set<TokenRange> unwrap(Set<TokenRange> tokenRanges)
    {
        ImmutableSet.Builder<TokenRange> result = ImmutableSet.builder();
        for (TokenRange range : tokenRanges) {
            result.addAll(range.unwrap());
        }
        return result.build();
    }

    public long getTotalPartitionsCount(String keyspace, String table, Optional<Long> sessionSplitsPerNode)
    {
        if (sessionSplitsPerNode.isPresent()) {
            return sessionSplitsPerNode.get();
        }
        if (configSplitsPerNode.isPresent()) {
            return configSplitsPerNode.get();
        }
        List<SizeEstimate> estimates = session.getSizeEstimates(keyspace, table);
        return estimates.stream()
                .mapToLong(SizeEstimate::getPartitionsCount)
                .sum();
    }

    private List<String> getEndpoints(String keyspace, TokenRange tokenRange)
    {
        Set<Node> endpoints = session.getReplicas(keyspace, tokenRange);

        return endpoints.stream()
                .map(endpoint -> endpoint.getEndPoint().resolve().toString())
                .collect(toImmutableList());
    }

    private static TokenSplit createSplit(TokenRange range, List<String> endpoints)
    {
        checkArgument(!range.isEmpty(), "tokenRange must not be empty");
        requireNonNull(range.getStart(), "tokenRange.start is null");
        requireNonNull(range.getEnd(), "tokenRange.end is null");
        return new TokenSplit(range, endpoints);
    }

    public static class TokenSplit
    {
        private TokenRange tokenRange;
        private List<String> hosts;

        public TokenSplit(TokenRange tokenRange, List<String> hosts)
        {
            this.tokenRange = requireNonNull(tokenRange, "tokenRange is null");
            this.hosts = ImmutableList.copyOf(requireNonNull(hosts, "hosts is null"));
        }

        public TokenRange getTokenRange()
        {
            return tokenRange;
        }

        public List<String> getHosts()
        {
            return hosts;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("startToken", tokenRange.getStart())
                    .add("endToken", tokenRange.getEnd())
                    .add("hosts", hosts)
                    .toString();
        }
    }
}
