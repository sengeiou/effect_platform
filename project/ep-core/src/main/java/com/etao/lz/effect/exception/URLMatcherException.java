package com.etao.lz.effect.exception;

public class URLMatcherException extends Exception {
	/**
	 * URL匹配异常
	 */
	private static final long serialVersionUID = 7238612207014792852L;

	public URLMatcherException() {
	}

	public URLMatcherException(String message) {
		super(message);
	}

	public URLMatcherException(Throwable cause) {
		super(cause);
	}

	public URLMatcherException(String message, Throwable cause) {
		super(message, cause);
	}
}
