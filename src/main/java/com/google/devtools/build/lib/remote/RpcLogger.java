package com.google.devtools.build.lib.remote;

import com.google.devtools.remoteexecution.v1test.RequestMetadata;
import com.google.protobuf.Message;
import io.grpc.Status;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.UUID;
import javax.annotation.Nullable;

public class RpcLogger {

  private final String name;
  private PrintWriter writer;
  private final Object streamLock = new Object();

  public RpcLogger(String name, OutputStream output) {
    this.name = name;
    this.writer = new PrintWriter(output);
    log("Logger started.");
  }

  public void logStart(UUID callId, String methodName, Message metadata) {
    log(callId, methodName, "Client call started with metadata", metadata.toString());
  }

  public void logStart(UUID callId, String methodName) {
    log(callId, methodName, "Client call started");
  }

  public void logReq(UUID callId, String methodName, Message msg) {
    log(callId, methodName, "Sent message", msg.toString());
  }

  public void logReq(UUID callId, String methodName) {
    log(callId, methodName, "Sent message");
  }

  public void logResp(UUID callId, String methodName, Message msg) {
    log(callId, methodName, "Received response", msg.toString());
  }

  public void logResp(UUID callId, String methodName) {
    log(callId, methodName, "Received response");
  }

  public void logClose(UUID callId, String methodName, Status status) {
    log(callId, methodName,"Call closed with status", status.toString());
  }

  public void logCancel(UUID callId, String methodName, @Nullable String message,
      @Nullable Throwable cause) {
    if (message == null && cause == null) {
      log(callId, methodName, "Call cancelled");
    } else {
      String details = message != null ? message : cause.toString();
      log(callId, methodName, "Call cancelled", details);
    }
  }

  private void log(UUID callId, String methodName, String description, String data) {
    String line = String.format(
        "%s [call-id: %s method: %s]:\n%s",
        description,
        callId,
        methodName,
        data
    );
    log(line);
  }

  private void log(UUID callId, String methodName, String description) {
    String line = String.format(
        "%s [call-id: %s method: %s]",
        description,
        callId,
        methodName
    );
    log(line);
  }

  private void log(String message) {
    String line = String.format("[%s] %s", name, message);
    writeLine(line);
  }

  private void writeLine(String line) {
    synchronized (streamLock) {
      writer.println(line);
      writer.flush();
    }
  }
}
