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
package io.trino.server.ui;

import io.airlift.configuration.Config;

public class WebUiConfig
{
    private boolean enabled = true;
    private boolean previewEnabled;

    public boolean isEnabled()
    {
        return enabled;
    }

    public boolean isPreviewEnabled()
    {
        return previewEnabled;
    }

    @Config("web-ui.enabled")
    public WebUiConfig setEnabled(boolean enabled)
    {
        this.enabled = enabled;
        return this;
    }

    @Config("web-ui.preview.enabled")
    public WebUiConfig setPreviewEnabled(boolean previewEnabled)
    {
        this.previewEnabled = previewEnabled;
        return this;
    }
}
