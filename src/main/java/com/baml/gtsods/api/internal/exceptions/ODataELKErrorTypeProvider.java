package com.baml.gtsods.api.internal.exceptions;

import org.mule.runtime.extension.api.annotation.error.ErrorTypeProvider;
import org.mule.runtime.extension.api.error.ErrorTypeDefinition;

import java.util.Set;
import java.util.HashSet;

public class ODataELKErrorTypeProvider implements ErrorTypeProvider {

    @Override
    public Set<ErrorTypeDefinition> getErrorTypes() {
        HashSet<ErrorTypeDefinition> errorTypes = new HashSet<>();
        errorTypes.add(ODataELKErrors.BAD_REQUEST);
        errorTypes.add(ODataELKErrors.INTERNAL_SERVER_ERROR);
        return errorTypes;
    }
}
