/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package co.cask.cdap.common.twill;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test cases for {@link TwillAppNames}.
 */
public class TwillAppNamesTest {

  @Test
  public void test() {
    ProgramId serviceId = new NamespaceId("test_ns").app("my_app").service("myservice");
    String twillAppName = TwillAppNames.toTwillAppName(serviceId);
    Assert.assertEquals("service.test_ns.my_app.myservice", twillAppName);
    Assert.assertEquals(serviceId, TwillAppNames.fromTwillAppName(twillAppName));

    // parsing from twill app name can be optional (return null)
    Assert.assertNull(TwillAppNames.fromTwillAppName(Constants.Service.MASTER_SERVICES, false));

    try {
      // if passing true, throws exception, when parsing is not possible
      TwillAppNames.fromTwillAppName(Constants.Service.MASTER_SERVICES, true);
      Assert.fail("Expected not being able to parse ");
    } catch (IllegalArgumentException e) {
      // expected
      Assert.assertTrue(e.getMessage().contains("does not match pattern for programs"));
    }
  }
}
