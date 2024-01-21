package io.bhex.broker.server.util;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
public class CompletableFutureUtil {

    private static final Integer COMPLETABLE_FUTURE_TIMEOUT = 7500;

    public static <T> void handleCompletableFutureResult(CompletableFuture<Boolean> result, String methodInfo, StreamObserver<T> observer, T timeoutResponse) {
        try {
            if (result.get(COMPLETABLE_FUTURE_TIMEOUT, TimeUnit.MILLISECONDS)) {
            } else {
                log.error("run CompletableFuture.async with:{} occurred error, please check", methodInfo);
                observer.onError(new RuntimeException("invoke statistics error!!! please check"));
            }
        } catch (Exception e) {
            log.error("run CompletableFuture.async with:{} occurred exception", methodInfo, e);
            if (e instanceof TimeoutException || (e.getCause() != null && e.getCause() instanceof TimeoutException)) {
                observer.onNext(timeoutResponse);
                observer.onCompleted();
            } else {
                observer.onError(e);
            }
        }
    }

    public static <T> void handleCompletableFutureException(Throwable e, String methodInfo, StreamObserver<T> observer, T timeoutResponse) {
        log.error("run CompletableFuture.async with:{} occurred {}", methodInfo, e);
        if (e instanceof RejectedExecutionException) {
            observer.onNext(timeoutResponse);
            observer.onCompleted();
        } else {
            observer.onError(e);
        }
    }

}
