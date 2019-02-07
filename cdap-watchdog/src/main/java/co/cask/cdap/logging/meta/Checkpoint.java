/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.logging.meta;

import com.google.common.base.Objects;

/**
 * Represents a checkpoint that can be saved when reading logs.
 * @param <Offset> type of the offset
 */
public class Checkpoint<Offset> {
  private final Offset offset;
  private final long maxEventTime;

  /**
   * Checkpoint containing offset and maxEventTime.
   */
  public Checkpoint(Offset offset, long maxEventTime) {
    this.offset = offset;
    this.maxEventTime = maxEventTime;
  }

  /**
   * Returns the offset.
   */
  public Offset getOffset() {
    return offset;
  }

  /**
   * Returns the max event time of persisted messages.
   */
  public long getMaxEventTime() {
    return maxEventTime;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("Offset", offset)
      .add("maxEventTime", maxEventTime)
      .toString();
  }
}
