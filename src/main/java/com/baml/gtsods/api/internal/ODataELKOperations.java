package com.baml.gtsods.api.internal;

import static org.mule.runtime.extension.api.annotation.param.MediaType.ANY;

import org.mule.runtime.extension.api.annotation.error.Throws;
import org.mule.runtime.extension.api.annotation.param.MediaType;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.Parameter;
import org.mule.runtime.extension.api.annotation.param.display.DisplayName;
import org.mule.runtime.extension.api.annotation.param.display.Example;
import org.mule.runtime.extension.api.exception.ModuleException;
import org.mule.runtime.api.component.ConfigurationProperties;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baml.gtsods.api.internal.exceptions.ODataELKErrorTypeProvider;
import com.baml.gtsods.api.internal.exceptions.ODataELKErrors;

/**
 * This class is a container for operations, every public method in this class
 * will be taken as an extension operation.
 */
public class ODataELKOperations {

	private final Logger logger = LoggerFactory.getLogger(ODataELKOperations.class);

	@Parameter
	@Example("log_*")
	@DisplayName("ELK Index")
	private String elkIndex;
	
	@Parameter
	@Example("name eq 'Naveen'")
	@DisplayName("Filter")
	private String filter;

	@Parameter
	@Optional(defaultValue = "*")
	@Example("field1,field2")
	@DisplayName("Select")
	private String select;

	@Parameter
	@Optional(defaultValue = "0")
	@Example("0")
	@DisplayName("Offset")
	private int offset;

	@Parameter
	@Optional(defaultValue = "500")
	@Example("500")
	@DisplayName("Max")
	private int top;

	@Inject
	private ConfigurationProperties configurationProperties;

	private String getProperty(String name) {
		return configurationProperties.resolveStringProperty(elkIndex + "." + name).orElse(null);
	}
	
	@MediaType(value = ANY, strict = false)
	@Throws(ODataELKErrorTypeProvider.class)
	public JSONObject parse()  {
		try {
		logger.info("Parsing filter: {}", filter);
		JSONObject result = parseOrExpr(new Tokenizer(filter));
		JSONObject finalResult = applyOptions(result, select, top, offset);
		// String resultString = finalResult.toString();
		logger.info("Parsed Elasticsearch DSL: {}", finalResult);
		return finalResult;
		}
		catch (ModuleException e) {
			throw e;
		}
		catch (Exception e) {
			throw new ModuleException("An unknown error occurred in ODATA ELK Module", ODataELKErrors.UNKNOWN_ERROR, e);
		}
	}

	private JSONObject parseOrExpr(Tokenizer tokenizer)  {
		List<JSONObject> orList = new ArrayList<>();
		do {
			orList.add(parseAndExpr(tokenizer));
		} while (tokenizer.consume("or"));

		if (orList.size() == 1) {
			return orList.get(0);
		}

		JSONObject orQuery = new JSONObject();
		orQuery.put("bool", new JSONObject().put("should", new JSONArray(orList)));
		return orQuery;
	}

	private JSONObject parseAndExpr(Tokenizer tokenizer)  {
		List<JSONObject> andList = new ArrayList<>();
		do {
			andList.add(parseComparisonExpr(tokenizer));
		} while (tokenizer.consume("and"));

		if (andList.size() == 1) {
			return andList.get(0);
		}

		JSONObject andQuery = new JSONObject();
		andQuery.put("bool", new JSONObject().put("must", new JSONArray(andList)));
		return andQuery;
	}

	private JSONObject parseComparisonExpr(Tokenizer tokenizer)  {
		if (tokenizer.consume("(")) {
			JSONObject expr = parseOrExpr(tokenizer);
			tokenizer.consume(")");
			return expr;
		}

		String keyName = tokenizer.next();
		String left = getProperty(keyName);
		if (left == null) {
			throw new ModuleException("Invalid input field '" + keyName + "'", ODataELKErrors.BAD_REQUEST);
		}
		String op = tokenizer.next();
		Object right = tokenizer.nextValue();

		JSONObject comparisonQuery = new JSONObject();
		switch (op) {
		case "eq":
			comparisonQuery.put("match_phrase", new JSONObject().put(left, right));
			break;
		case "ne":
			comparisonQuery.put("bool", new JSONObject().put("must_not",
					new JSONObject().put("match_phrase", new JSONObject().put(left, right))));
			break;
		case "gt":
			comparisonQuery.put("range", new JSONObject().put(left, new JSONObject().put("gt", right)));
			break;
		case "ge":
			comparisonQuery.put("range", new JSONObject().put(left, new JSONObject().put("gte", right)));
			break;
		case "lt":
			comparisonQuery.put("range", new JSONObject().put(left, new JSONObject().put("lt", right)));
			break;
		case "le":
			comparisonQuery.put("range", new JSONObject().put(left, new JSONObject().put("lte", right)));
			break;
		default:
			throw new IllegalArgumentException("Unsupported comparison operator: " + op);
		}
		return comparisonQuery;
	}

	private JSONObject applyOptions(JSONObject query, String select, Integer top, Integer offset) {
		JSONObject options = new JSONObject();
		options.put("query", query);

		if (select != null && !select.isEmpty()) {
			query.put("_source", new JSONObject().put("includes", new JSONArray(select.split(","))));
		}
		if (top != null) {
			query.put("size", top);
		}
		if (offset != null) {
			query.put("from", offset);
		}
		return options;
	}

	private class Tokenizer {
		private final List<String> tokens;
		private int index = 0;

		public Tokenizer(String input) {
			tokens = tokenize(input);
		}

		private List<String> tokenize(String input) {
			List<String> tokens = new ArrayList<>();
			StringBuilder token = new StringBuilder();
			boolean inQuotes = false;

			for (char ch : input.toCharArray()) {
				if (ch == '\'') {
					inQuotes = !inQuotes;
				}
				if (Character.isWhitespace(ch) && !inQuotes) {
					if (token.length() > 0) {
						tokens.add(token.toString());
						token.setLength(0);
					}
				} else if (!inQuotes && (ch == '(' || ch == ')' || ch == ',')) {
					if (token.length() > 0) {
						tokens.add(token.toString());
						token.setLength(0);
					}
					tokens.add(String.valueOf(ch));
				} else {
					token.append(ch);
				}
			}

			if (token.length() > 0) {
				tokens.add(token.toString());
			}

			return tokens;
		}

		public boolean consume(String token) {
			if (index < tokens.size() && tokens.get(index).equalsIgnoreCase(token)) {
				index++;
				return true;
			}
			return false;
		}

		public String next() {
			if (index < tokens.size()) {
				return tokens.get(index++);
			}
			throw new IllegalStateException("Unexpected end of input");
		}

		public Object nextValue() {
			if (index < tokens.size()) {
				String value = tokens.get(index++);
				if (value.startsWith("'") && value.endsWith("'")) {
					return value.substring(1, value.length() - 1);
				}
				try {
					return Double.parseDouble(value); // Return as a number if possible
				} catch (NumberFormatException e) {
					return value; // Return as a string if it's not a number
				}
			}
			throw new IllegalStateException("Unexpected end of input");
		}
	}
}
