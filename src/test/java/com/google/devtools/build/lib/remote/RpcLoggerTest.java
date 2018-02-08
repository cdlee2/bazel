package com.google.devtools.build.lib.remote;

import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.protobuf.Duration;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.ByteArrayOutputStream;
import java.util.UUID;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link RpcLoggerTest}. */
@RunWith(JUnit4.class)
public class RpcLoggerTest {

  private final String loggerName = "TestLogger";
  private final UUID testUUID = UUID.fromString("b11423bf-02f6-465c-834f-1337b5870d06");

  @Test
  public void testCallStarted() {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    RpcLogger logger = new RpcLogger(loggerName, out);
    logger.logStart(testUUID, "testMethod");
    String log = new String(out.toByteArray(), UTF_8);
    assertThat(log).startsWith("[TestLogger] Logger started.");
    assertThat(log).contains("method: testMethod");
    assertThat(log).contains("call-id: " + testUUID.toString());
    assertThat(log).contains("Client call started");
  }

  @Test
  public void testLogReqWithData() {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    RpcLogger logger = new RpcLogger(loggerName, out);
    Duration testMessage = Duration.newBuilder().setNanos(1234).setSeconds(56).build();
    logger.logReq(testUUID, "testMethod", testMessage);
    String log = new String(out.toByteArray(), UTF_8);
    assertThat(log).startsWith("[TestLogger] Logger started.");
    assertThat(log).contains("method: testMethod");
    assertThat(log).contains("call-id: " + testUUID.toString());
    assertThat(log).contains("seconds: 56\nnanos: 1234");
  }

  @Test
  public void testLogReqWithoutData() {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    RpcLogger logger = new RpcLogger(loggerName, out);
    logger.logReq(testUUID, "testMethod");
    String log = new String(out.toByteArray(), UTF_8);
    assertThat(log).startsWith("[TestLogger] Logger started.");
    assertThat(log).contains("method: testMethod");
    assertThat(log).contains("call-id: " + testUUID.toString());
  }

  @Test
  public void testLogRespWithData() {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    RpcLogger logger = new RpcLogger(loggerName, out);
    Duration testMessage = Duration.newBuilder().setNanos(1234).setSeconds(56).build();
    logger.logResp(testUUID, "testMethod", testMessage);
    String log = new String(out.toByteArray(), UTF_8);
    assertThat(log).startsWith("[TestLogger] Logger started.");
    assertThat(log).contains("method: testMethod");
    assertThat(log).contains("call-id: " + testUUID.toString());
    assertThat(log).contains("seconds: 56\nnanos: 1234");
  }

  @Test
  public void testLogRespWithoutData() {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    RpcLogger logger = new RpcLogger(loggerName, out);
    logger.logResp(testUUID, "testMethod");
    String log = new String(out.toByteArray(), UTF_8);
    assertThat(log).startsWith("[TestLogger] Logger started.");
    assertThat(log).contains("method: testMethod");
    assertThat(log).contains("call-id: " + testUUID.toString());
  }

  @Test
  public void testLogCancelWithMessage() {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    RpcLogger logger = new RpcLogger(loggerName, out);
    logger.logCancel(testUUID, "testMethod", "error message", null);
    String log = new String(out.toByteArray(), UTF_8);

    assertThat(log).startsWith("[TestLogger] Logger started.");
    assertThat(log).contains("method: testMethod");
    assertThat(log).contains("call-id: " + testUUID.toString());
    assertThat(log).contains("error message");
  }

  @Test
  public void testLogCancelWithThrowable() {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    RpcLogger logger = new RpcLogger(loggerName, out);
    StatusRuntimeException error = Status.NOT_FOUND.withDescription("error").asRuntimeException();
    logger.logCancel(testUUID, "testMethod", null, error);
    String log = new String(out.toByteArray(), UTF_8);
    assertThat(log).startsWith("[TestLogger] Logger started.");
    assertThat(log).contains("method: testMethod");
    assertThat(log).contains("call-id: " + testUUID.toString());
    assertThat(log).contains("NOT_FOUND: error");
  }

  @Test
  public void testLogCancelWithNoMessage() {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    RpcLogger logger = new RpcLogger(loggerName, out);
    logger.logCancel(testUUID, "testMethod", null, null);
    String log = new String(out.toByteArray(), UTF_8);
    assertThat(log).startsWith("[TestLogger] Logger started.");
    assertThat(log).contains("method: testMethod");
    assertThat(log).contains("call-id: " + testUUID.toString());
    assertThat(log).contains("Call cancelled");
  }

  @Test
  public void testLogClose() {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    RpcLogger logger = new RpcLogger(loggerName, out);
    Status error = Status.NOT_FOUND.withDescription("error");
    logger.logClose(testUUID, "testMethod", error);
    String log = new String(out.toByteArray(), UTF_8);
    assertThat(log).startsWith("[TestLogger] Logger started.");
    assertThat(log).contains("method: testMethod");
    assertThat(log).contains("call-id: " + testUUID.toString());
    assertThat(log).contains("code=NOT_FOUND, description=error");
  }
}
