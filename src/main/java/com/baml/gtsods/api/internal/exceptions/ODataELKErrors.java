package com.baml.gtsods.api.internal.exceptions;

import org.mule.runtime.extension.api.error.ErrorTypeDefinition;

public enum ODataELKErrors implements ErrorTypeDefinition<ODataELKErrors> {
    BAD_REQUEST,
    INTERNAL_SERVER_ERROR;
}
