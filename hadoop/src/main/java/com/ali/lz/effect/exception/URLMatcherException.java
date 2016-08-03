package com.ali.lz.effect.exception;

/**
 * URL匹配异常
 */
public class URLMatcherException extends Exception {

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
