/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.sink.elasticsearch;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Basic serializer that serializes the event body and header fields into
 * individual fields</p>
 *
 * A best effort will be used to determine the content-type, if it cannot be
 * determined fields will be indexed as Strings
 */
public class ElasticSearchSoucheLogSerializer implements
    ElasticSearchEventSerializer {

  @Override
  public void configure(Context context) {
    // NO-OP...
  }

  @Override
  public void configure(ComponentConfiguration conf) {
    // NO-OP...
  }

  @Override
  public XContentBuilder getContentBuilder(Event event) throws IOException {
    XContentBuilder builder = jsonBuilder().startObject();
    appendBody(builder, event);
    appendHeaders(builder, event);
    return builder;
  }

  private void appendBody(XContentBuilder builder, Event event)
      throws IOException {
    String body = new String(event.getBody());
    String[] segs = body.split(ElasticSearchSinkConstants.SOUCHE_LOG_SEPERATOR);
    for (int i = 0; i < segs.length; i++) {
      String seg = segs[i].trim();
      if (i == 0) {
        /*
         * 2017-06-21 10:41:25.138 [DubboServerHandler-121.*.*.*:20898-thread-6]
         */
        // get the date such as: 2017-06-21 10:41:25.138
        String date = seg.substring(0, 23);
        ContentBuilderUtil.appendField(builder, "date", date.getBytes());
        String threadName = seg.substring(23).trim();
        ContentBuilderUtil.appendField(builder, "threadName", threadName.getBytes());
        continue;
      }
      String[] kvs = seg.split(":", 2);
      if (kvs.length > 0) {
        String k = kvs[0].trim();
        String v = null;
        if (kvs.length > 1) {
          v = kvs[1].trim();
        }
        else {
          v = "";
        }
        ContentBuilderUtil.appendField(builder, k, v.getBytes());
      }
    }
  }

  private void appendHeaders(XContentBuilder builder, Event event)
      throws IOException {
    Map<String, String> headers = event.getHeaders();
    for (String key : headers.keySet()) {
      ContentBuilderUtil.appendField(builder, key,
          headers.get(key).getBytes(charset));
    }
  }

}
