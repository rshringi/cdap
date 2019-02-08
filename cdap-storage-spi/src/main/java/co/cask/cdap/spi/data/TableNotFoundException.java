/*
 * Copyright © 2019 Cask Data, Inc.
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
 *
 */

package co.cask.cdap.spi.data;

import co.cask.cdap.spi.data.table.StructuredTableId;

import java.io.IOException;

/**
 * Thrown when a table does not exist when it is expected to.
 */
public class TableNotFoundException extends IOException {
  private final StructuredTableId id;

  public TableNotFoundException(StructuredTableId id) {
    super(String.format("System table '%s' not found.", id));
    this.id = id;
  }

  public StructuredTableId getId() {
    return id;
  }
}
