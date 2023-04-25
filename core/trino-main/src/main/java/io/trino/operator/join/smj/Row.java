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
package io.trino.operator.join.smj;

import io.trino.spi.Page;

import java.util.List;

public class Row
{
    private Page page;
    private int position;

    private List<Integer> equiJoinClauseChannels;

    public Row()
    {
    }

    public Row(Page page, int position, List<Integer> equiJoinClauseChannels)
    {
        this.page = page;
        this.position = position;
        this.equiJoinClauseChannels = equiJoinClauseChannels;
    }

    public void set(Page page, int position)
    {
        this.page = page;
        this.position = position;
    }

    public void reset()
    {
        this.page = null;
        this.position = -1;
    }

    @Override
    public Row clone()
    {
        return new Row(page, position, equiJoinClauseChannels);
    }

    public boolean isExist()
    {
        return page != null;
    }

    public Page getPage()
    {
        return page;
    }

    public int getPosition()
    {
        return position;
    }

    public List<Integer> getEquiJoinClauseChannels()
    {
        return equiJoinClauseChannels;
    }

    public void setEquiJoinClauseChannels(List<Integer> equiJoinClauseChannels)
    {
        this.equiJoinClauseChannels = equiJoinClauseChannels;
    }
}
