# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM python:3.12-slim

# Install required dependencies
RUN apt-get update -y && apt-get install -y \
    zlib1g-dev \
    libjpeg-dev \
    libxml2-dev \
    libxslt-dev \
    build-essential \
    xsltproc 

# Setup working directory and cache directory
WORKDIR /docs
RUN mkdir -p /.cache && chmod 777 /.cache

# Install Python dependencies
COPY requirements.txt /docs/
RUN pip3 install --no-cache-dir -r requirements.txt