package com.baml.gtsods.api.internal;

public class BadRequestException extends Exception {
	private static final long serialVersionUID = 4747353704329520033L;

	public BadRequestException(String message) {
		super(message);
	}
}
