package com.xing.beetle;

/**
 *
 */
public class HandlerResponse {

    public static final HandlerResponse OK = new HandlerResponse(ResponseCode.OK);
    public static final HandlerResponse INTERRUPTED = new HandlerResponse(ResponseCode.INTERRUPTED);
    public static final HandlerResponse EXCEPTION = new HandlerResponse(ResponseCode.EXCEPTION);

    public static enum ResponseCode {
        OK,
        INTERRUPTED,
        EXCEPTION, ANCIENT
    }

    private ResponseCode responseCode;

    public HandlerResponse(ResponseCode responseCode) {

        this.responseCode = responseCode;
    }

    public ResponseCode getResponseCode() {
        return responseCode;
    }

    public void setResponseCode(ResponseCode responseCode) {
        this.responseCode = responseCode;
    }

    public boolean isSuccess() {
        return responseCode == ResponseCode.OK;
    }
}
