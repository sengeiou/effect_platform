package com.etao.lz.effect.exception;

public class HoloConfigParserException extends Exception {

	/**
	 *  HoloConfig文件解析异常信息
	 */
	private static final long serialVersionUID = 653992676652547584L;

	public HoloConfigParserException() {
	}

	public HoloConfigParserException(String message) {
		super(message);
	}

	public HoloConfigParserException(Throwable cause) {
		super(cause);
	}

	public HoloConfigParserException(String message, Throwable cause) {
		super(message, cause);
	}

}
