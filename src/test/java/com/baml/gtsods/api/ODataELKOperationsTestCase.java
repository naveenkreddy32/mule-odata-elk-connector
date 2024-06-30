package com.baml.gtsods.api;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import org.mule.functional.junit4.MuleArtifactFunctionalTestCase;
import org.mule.runtime.core.api.event.CoreEvent;
import org.mule.runtime.extension.api.exception.ModuleException;
import org.mule.tck.junit4.matcher.ErrorTypeMatcher;
import static org.mule.tck.junit4.matcher.ErrorTypeMatcher.errorType;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class ODataELKOperationsTestCase extends MuleArtifactFunctionalTestCase {

	/**
	 * Specifies the mule config xml with the flows that are going to be executed in
	 * the tests, this file lives in the test resources.
	 */
	@Override
	protected String getConfigFile() {
		return "test-mule-config.xml";
	}

	@Test
	public void simpleFilter() throws Exception {
		String filter = "name eq 'Naveen'";
		CoreEvent parse = flowRunner("parse").withPayload(filter).run();
		String payloadValue = (String) (parse.getMessage().getPayload().getValue());

		assertThat(payloadValue, is(
				"{\"query\":{\"match_phrase\":{\"newName\":\"Naveen\"},\"size\":500,\"_source\":{\"includes\":[\"*\"]},\"from\":0}}"));
	}
	
	@Test
	public void simpleFilterWithSelect() throws Exception {
		String filter = "name eq 'Naveen'";
		String sel = "name";
		CoreEvent parse = flowRunner("parse").withPayload(filter).withVariable("select", sel).run();
		String payloadValue = (String) (parse.getMessage().getPayload().getValue());

		assertThat(payloadValue, is(
				"{\"query\":{\"match_phrase\":{\"newName\":\"Naveen\"},\"size\":500,\"_source\":{\"includes\":[\"newName\"]},\"from\":0}}"));
	}

	@Test
	public void validateFilterBadRequestErrType() throws Exception {
		String filter = "lname eq 'Naveen'";
		flowRunner("parse").withPayload(filter).runExpectingException(errorType("ODATAELK", "BAD_REQUEST"));
	}

	@Test
	public void validateFilterBadRequestErrMsg() throws Exception {
		String filter = "lname eq 'Naveen'";
		Exception e = flowRunner("parse").withPayload(filter).runExpectingException();
		assertThat(e.getMessage(), is("Invalid filter field 'lname'. Please check if its correct/present in YAML file."));

	}

	@Test
	public void validateSelectBadRequestErrType() throws Exception {
		String filter = "name eq 'Naveen'";
		String sel = "lname";
		flowRunner("parse").withPayload(filter).withVariable("select", sel)
				.runExpectingException(errorType("ODATAELK", "BAD_REQUEST"));
	}

	@Test
	public void validateSelectBadRequestErrMsg() throws Exception {
		String filter = "name eq 'Naveen'";
		String sel = "lname";
		Exception e = flowRunner("parse").withPayload(filter).withVariable("select", sel).runExpectingException();
		assertThat(e.getMessage(), is("Invalid select field 'lname'. Please check if its correct/present in YAML file."));

	}
}
