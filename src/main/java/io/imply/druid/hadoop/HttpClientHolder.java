/*
 * Copyright 2016 Imply Data, Inc.
 *
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

package io.imply.druid.hadoop;

import com.google.common.base.Throwables;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.HttpClientConfig;
import com.metamx.http.client.HttpClientInit;

import java.io.Closeable;
import java.io.IOException;

public class HttpClientHolder implements Closeable
{
  private final Lifecycle lifecycle;
  private final HttpClient client;

  public HttpClientHolder(Lifecycle lifecycle, HttpClient client)
  {
    this.lifecycle = lifecycle;
    this.client = client;
  }

  public static HttpClientHolder create()
  {
    final Lifecycle lifecycle = new Lifecycle();
    final HttpClient httpClient = HttpClientInit.createClient(
        HttpClientConfig.builder().build(),
        lifecycle
    );

    try {
      lifecycle.start();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    return new HttpClientHolder(lifecycle, httpClient);
  }

  public HttpClient get()
  {
    return client;
  }

  @Override
  public void close() throws IOException
  {
    lifecycle.stop();
  }
}
