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
package io.trino.node;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public record AllNodes(
        Set<InternalNode> activeNodes,
        Set<InternalNode> inactiveNodes,
        Set<InternalNode> drainingNodes,
        Set<InternalNode> drainedNodes,
        Set<InternalNode> shuttingDownNodes,
        Set<InternalNode> activeCoordinators)
{
    public AllNodes
    {
        activeNodes = ImmutableSet.copyOf(requireNonNull(activeNodes, "activeNodes is null"));
        inactiveNodes = ImmutableSet.copyOf(requireNonNull(inactiveNodes, "inactiveNodes is null"));
        drainedNodes = ImmutableSet.copyOf(requireNonNull(drainedNodes, "drainedNodes is null"));
        drainingNodes = ImmutableSet.copyOf(requireNonNull(drainingNodes, "drainingNodes is null"));
        shuttingDownNodes = ImmutableSet.copyOf(requireNonNull(shuttingDownNodes, "shuttingDownNodes is null"));
        activeCoordinators = ImmutableSet.copyOf(requireNonNull(activeCoordinators, "activeCoordinators is null"));
    }
}
