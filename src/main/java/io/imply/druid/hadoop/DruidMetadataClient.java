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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.net.HostAndPort;
import com.metamx.common.ISE;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.timeline.DataSegment;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.Interval;

import javax.ws.rs.core.MediaType;
import java.net.URL;
import java.net.URLEncoder;
import java.util.List;

public class DruidMetadataClient
{
  private final HttpClient httpClient;
  private final ObjectMapper objectMapper;
  private final HostAndPort hostAndPort;

  public DruidMetadataClient(HttpClient httpClient, ObjectMapper objectMapper, HostAndPort hostAndPort)
  {
    this.httpClient = httpClient;
    this.objectMapper = objectMapper;
    this.hostAndPort = hostAndPort;
  }

  public List<DataSegment> usedSegments(
      final String dataSource,
      final List<Interval> intervals
  )
  {
    try {
      final Request request = new Request(
          HttpMethod.POST,
          new URL(
              String.format(
                  "http://%s/druid/coordinator/v1/metadata/datasources/%s/segments?full",
                  hostAndPort,
                  URLEncoder.encode(dataSource, "UTF-8")
              )
          )
      );
      request.setHeader("Content-Type", MediaType.APPLICATION_JSON);
      request.setContent(objectMapper.writeValueAsBytes(intervals));

      final StatusResponseHolder response = httpClient.go(
          request,
          new StatusResponseHandler(Charsets.UTF_8)
      ).get();

      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE(
            "Error while fetching segments, status[%s] content[%s]",
            response.getStatus(),
            response.getContent()
        );
      }

      return objectMapper.readValue(
          response.getContent(),
          new TypeReference<List<DataSegment>>()
          {
          }
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
