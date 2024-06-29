package com.baml.gtsods.api;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import org.mule.functional.junit4.MuleArtifactFunctionalTestCase;
import org.junit.Test;

public class ODataELKOperationsTestCase extends MuleArtifactFunctionalTestCase {

  /**
   * Specifies the mule config xml with the flows that are going to be executed in the tests, this file lives in the test resources.
   */
  @Override
  protected String getConfigFile() {
    return "test-mule-config.xml";
  }

  @Test
  public void executeSayHiOperation() throws Exception {
    String payloadValue = (flowRunner("parse").run()
                                      .getMessage()
                                      .getPayload()
                                      .getValue()).toString();

    assertThat(payloadValue, is("{\"query\":{\"match_phrase\":{\"newName\":\"Naveen\"},\"size\":500,\"_source\":{\"includes\":[\"*\"]},\"from\":0}}"));
  }
}
