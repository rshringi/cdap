/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.proto;

import java.util.List;

/**
 * A live info for in-memory runtime envirnment.
 */
public class InMemoryProgramLiveInfo extends ProgramLiveInfo {
  private final List<String> discoverables;

  public InMemoryProgramLiveInfo(Id.Program programId, ProgramType type) {
    this(programId, type, null);
  }

  public InMemoryProgramLiveInfo(Id.Program programId, ProgramType type, List<String> discoverables) {
    super(programId, type, "in-memory");
    this.discoverables = discoverables;
  }
}
