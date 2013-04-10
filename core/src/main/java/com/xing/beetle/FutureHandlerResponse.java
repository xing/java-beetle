package com.xing.beetle;

import java.util.concurrent.*;

/**
 *
 */
public class FutureHandlerResponse extends FutureTask<HandlerResponse> {

    public FutureHandlerResponse(Callable<HandlerResponse> callable) {
        super(callable);
    }

    public HandlerResponse responseGet() {
        try {
            return get();
        } catch (InterruptedException e) {
            setException(e);
            return HandlerResponse.INTERRUPTED;
        } catch (ExecutionException e) {
            setException(e.getCause());
            return HandlerResponse.EXCEPTION;
        }
    }
}
