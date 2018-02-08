package com.google.devtools.build.lib.remote;

import static com.google.devtools.build.lib.remote.TracingMetadataUtils.extractRequestMetadata;
import static java.util.UUID.randomUUID;

import com.google.devtools.remoteexecution.v1test.RequestMetadata;
import com.google.protobuf.Message;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.util.UUID;
import javax.annotation.Nullable;

public class LoggingInterceptor implements ClientInterceptor {
  private RpcLogger rpcLogger;
  private boolean logData;

  LoggingInterceptor(RpcLogger rpcLogger) {
    this(rpcLogger, true);
  }

  LoggingInterceptor(RpcLogger rpcLogger, boolean logData) {
    this.rpcLogger = rpcLogger;
    this.logData = logData;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
    return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
        next.newCall(method, callOptions)) {
      String methodName = method.getFullMethodName();
      UUID callId = randomUUID();
      @Override
      public void start(Listener<RespT> responseListener, Metadata headers) {
        RequestMetadata metadata;
        try {
          metadata = extractRequestMetadata(headers);
        } catch (IllegalStateException e) {
          metadata = null;
        }
        if (metadata != null) {
          rpcLogger.logStart(callId, methodName, metadata);
        } else {
          rpcLogger.logStart(callId, methodName);
        }
        super.start(
            new ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(
                responseListener) {
              @Override
              public void onMessage(RespT message) {
                if (logData) {
                  rpcLogger.logResp(callId, methodName, (Message) message);
                } else {
                  rpcLogger.logResp(callId, methodName);
                }
                super.onMessage(message);
              }

              @Override
              public void onClose(Status status, Metadata trailers) {
                rpcLogger.logClose(callId, methodName, status);
                super.onClose(status, trailers);
              }
            },
            headers);
      }

      @Override
      public void sendMessage(ReqT message) {
        if (logData) {
          rpcLogger.logReq(callId, methodName, (Message) message);
        } else {
          rpcLogger.logReq(callId, methodName);
        }
        super.sendMessage(message);
      }

      @Override
      public void cancel(@Nullable String message, @Nullable Throwable cause) {
        rpcLogger.logCancel(callId, methodName, message, cause);
        super.cancel(message, cause);
      }
    };
  }
}
