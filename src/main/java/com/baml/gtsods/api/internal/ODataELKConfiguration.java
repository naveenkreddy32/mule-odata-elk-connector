package com.baml.gtsods.api.internal;

import org.mule.runtime.extension.api.annotation.Operations;
import org.mule.runtime.extension.api.annotation.param.Parameter;

/**
 * This class represents an extension configuration, values set in this class are commonly used across multiple
 * operations since they represent something core from the extension.
 */
@Operations(ODataELKOperations.class)
public class ODataELKConfiguration {

  @Parameter
  private String configId;

  public String getConfigId(){
    return configId;
  }
}
