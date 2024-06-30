package com.baml.gtsods.api.internal;

import static org.mule.runtime.extension.api.annotation.param.MediaType.ANY;

import org.mule.runtime.extension.api.annotation.Alias;
import org.mule.runtime.extension.api.annotation.Expression;
import org.mule.runtime.extension.api.annotation.error.Throws;
import org.mule.runtime.extension.api.annotation.param.MediaType;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.Parameter;
import org.mule.runtime.extension.api.annotation.param.display.DisplayName;
import org.mule.runtime.extension.api.annotation.param.display.Example;
import org.mule.runtime.extension.api.annotation.param.display.Summary;
import org.mule.runtime.extension.api.exception.ModuleException;
import org.mule.runtime.api.component.ConfigurationProperties;
import org.mule.runtime.api.meta.ExpressionSupport;
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

	@Inject
	private ConfigurationProperties configurationProperties;

	private String getProperty(String name) {
		logger.debug("resolvingProperty: {}", name);
		return configurationProperties.resolveStringProperty(name).orElse(null);
	}

	@MediaType(value = ANY, strict = false)
	@Throws(ODataELKErrorTypeProvider.class)
	@Alias("transform-to-elk-bool")
	public String generateELKDSLQuery(
			@Expression(ExpressionSupport.SUPPORTED) @Example("log_*") @DisplayName("ELK Index Fields") @Summary("Configuration property prefix for validating input fields") @Alias("propPrefix") String propPrefix,

			@Expression(ExpressionSupport.SUPPORTED) @Example("name eq 'Naveen'") @DisplayName("Filter") @Summary("Filter string with logical and binary operators to filter data") @Alias("filter") String filter,

			@Expression(ExpressionSupport.SUPPORTED) @Optional(defaultValue = "*") @Example("field1,field2") @DisplayName("Select") @Summary("Comma separated field names to be shown in response.") @Alias("select") String select,

			@Expression(ExpressionSupport.SUPPORTED) @Optional(defaultValue = "0") @Example("0") @DisplayName("Offset") @Summary("Option to request the number of items in the queried collection that are to be skipped and not included in the result.") @Alias("offset") int offset,

			@Expression(ExpressionSupport.SUPPORTED) @Optional(defaultValue = "500") @Example("500") @DisplayName("Max") @Summary("Option requests the number of items in the queried collection to be included in the result") @Alias("top") int top) {
		try {
			logger.debug("Parsing filter: {}, select: {}, offest: {}, max: {}", filter, select, offset, top);
			JSONObject result = parseOrExpr(new Tokenizer(filter), propPrefix);
			JSONObject finalResult = applyOptions(result, select, top, offset, propPrefix);
			String resultString = finalResult.toString();
			logger.debug("Generated Elasticsearch DSL: {}", resultString);
			return resultString;
		} catch (ModuleException e) {
			throw e;
		} catch (Exception e) {
			throw new ModuleException("An unknown error occurred in ODATA ELK Module",
					ODataELKErrors.INTERNAL_SERVER_ERROR, e);
		}
	}

	private JSONObject parseOrExpr(Tokenizer tokenizer, String propPrefix) {
		List<JSONObject> orList = new ArrayList<>();
		do {
			orList.add(parseAndExpr(tokenizer, propPrefix));
		} while (tokenizer.consume("or"));

		if (orList.size() == 1) {
			return orList.get(0);
		}

		JSONObject orQuery = new JSONObject();
		orQuery.put("bool", new JSONObject().put("should", new JSONArray(orList)));
		logger.debug("parseOrExpr: {}", orQuery);
		return orQuery;
	}

	private JSONObject parseAndExpr(Tokenizer tokenizer, String propPrefix) {
		List<JSONObject> andList = new ArrayList<>();
		do {
			andList.add(parseComparisonExpr(tokenizer, propPrefix));
		} while (tokenizer.consume("and"));

		if (andList.size() == 1) {
			return andList.get(0);
		}

		JSONObject andQuery = new JSONObject();
		andQuery.put("bool", new JSONObject().put("must", new JSONArray(andList)));
		logger.debug("parseAndExpr: {}", andQuery);
		return andQuery;
	}

	private JSONObject parseComparisonExpr(Tokenizer tokenizer, String propPrefix) {
		if (tokenizer.consume("(")) {
			JSONObject expr = parseOrExpr(tokenizer, propPrefix);
			tokenizer.consume(")");
			return expr;
		}

		String keyName = tokenizer.next();
		String left = getProperty(propPrefix + "." + keyName);
		if (left == null) {
			throw new ModuleException("Invalid filter field '" + keyName + "'. Please check if its correct/present in YAML file",
					ODataELKErrors.BAD_REQUEST);
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
		logger.debug("parseComparisonExpr: {}", comparisonQuery);
		return comparisonQuery;
	}

	private JSONObject applyOptions(JSONObject query, String select, Integer top, Integer offset, String propPrefix) {
		JSONObject options = new JSONObject();
		options.put("query", query);

		if (select.equals("*")) {
			query.put("_source", new JSONObject().put("includes", new JSONArray(select.split(","))));
		} else {
			List<String> selectList = new ArrayList<>();

			for (String field : select.split(",")) {
				String mappedField = getProperty(propPrefix + "." + field);
				if (mappedField == null) {
					throw new ModuleException("Invalid select field '" + field + "'. Please check if its correct/present in YAML file",
							ODataELKErrors.BAD_REQUEST);
				}
				selectList.add(mappedField);
			}
			query.put("_source", new JSONObject().put("includes", new JSONArray(selectList)));
		}

		if (top != null) {
			query.put("size", top);
		}
		if (offset != null) {
			query.put("from", offset);
		}
		logger.debug("applyOptions: {}", options);
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
			logger.debug("tokenize: {}", tokens);
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
