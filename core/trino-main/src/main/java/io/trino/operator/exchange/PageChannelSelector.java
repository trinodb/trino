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
package io.trino.operator.exchange;

import io.trino.spi.Page;

import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;

public class PageChannelSelector
        implements Function<Page, Page>
{
    // No channels need to be remapped, only ensure that all page blocks are loaded
    private static final Function<Page, Page> GET_LOADED_PAGE = Page::getLoadedPage;

    private final int[] channels;
    private final boolean keepSpooledMetadata;

    public PageChannelSelector(boolean keepSpooledMetadata, int... channels)
    {
        this.keepSpooledMetadata = keepSpooledMetadata;
        this.channels = channels.clone();
        checkArgument(IntStream.of(channels).allMatch(channel -> channel >= 0), "channels must be positive");
    }

    @Override
    public Page apply(Page page)
    {
        int[] channelsToKeep = channels;
        if (keepSpooledMetadata) {
            channelsToKeep = Arrays.copyOf(channels, channels.length + 1);
            channelsToKeep[channelsToKeep.length - 1] = page.getChannelCount() - 1; // keep last block of spooled metadata
        }
        // Ensure the channels that are emitted are fully loaded and in the correct order
        return page.getLoadedPage(channelsToKeep);
    }

    public static Function<Page, Page> identitySelection()
    {
        return GET_LOADED_PAGE;
    }
}
