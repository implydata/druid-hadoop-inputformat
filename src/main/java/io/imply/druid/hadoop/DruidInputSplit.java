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

import com.google.common.base.Preconditions;
import io.druid.indexer.hadoop.WindowedDataSegment;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DruidInputSplit extends InputSplit implements Writable
{
  private static final String[] EMPTY_LOCATIONS = new String[]{};

  private WindowedDataSegment segment = null;

  public DruidInputSplit()
  {
  }

  public static DruidInputSplit create(WindowedDataSegment segment)
  {
    final DruidInputSplit split = new DruidInputSplit();
    split.segment = Preconditions.checkNotNull(segment, "segment");
    return split;
  }

  @Override
  public long getLength() throws IOException, InterruptedException
  {
    return segment.getSegment().getSize();
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException
  {
    return EMPTY_LOCATIONS;
  }

  public WindowedDataSegment getSegment()
  {
    return segment;
  }

  @Override
  public void write(final DataOutput out) throws IOException
  {
    final byte[] segmentBytes = DruidInputFormat.objectMapper().writeValueAsBytes(segment);
    out.writeInt(segmentBytes.length);
    out.write(segmentBytes);
  }

  @Override
  public void readFields(final DataInput in) throws IOException
  {
    final int segmentBytesLength = in.readInt();
    final byte[] segmentBytes = new byte[segmentBytesLength];
    in.readFully(segmentBytes);
    this.segment = DruidInputFormat.objectMapper().readValue(segmentBytes, WindowedDataSegment.class);
  }
}
