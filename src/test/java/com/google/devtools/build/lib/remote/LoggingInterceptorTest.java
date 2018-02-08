package com.google.devtools.build.lib.remote;

import static com.google.common.collect.Iterators.advance;
import static com.google.common.truth.Truth.assertThat;
import static com.google.devtools.build.lib.remote.TracingMetadataUtils.METADATA_KEY;
import static com.google.devtools.build.lib.testutil.MoreAsserts.assertThrows;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.contains;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.bytestream.ByteStreamGrpc;
import com.google.bytestream.ByteStreamGrpc.ByteStreamBlockingStub;
import com.google.bytestream.ByteStreamGrpc.ByteStreamImplBase;
import com.google.bytestream.ByteStreamProto.ReadRequest;
import com.google.bytestream.ByteStreamProto.ReadResponse;
import com.google.devtools.remoteexecution.v1test.ActionCacheGrpc;
import com.google.devtools.remoteexecution.v1test.ActionCacheGrpc.ActionCacheBlockingStub;
import com.google.devtools.remoteexecution.v1test.ActionCacheGrpc.ActionCacheImplBase;
import com.google.devtools.remoteexecution.v1test.ActionResult;
import com.google.devtools.remoteexecution.v1test.GetActionResultRequest;
import com.google.devtools.remoteexecution.v1test.RequestMetadata;
import com.google.protobuf.ByteString;
import com.google.watcher.v1.Request;
import io.grpc.Channel;
import io.grpc.ClientInterceptor;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.grpc.util.MutableHandlerRegistry;
import java.util.Iterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link LoggingInterceptor}. */
@RunWith(JUnit4.class)
public class LoggingInterceptorTest {
  private final String fakeServerName = "fake server for " + getClass();
  private final MutableHandlerRegistry serviceRegistry = new MutableHandlerRegistry();
  private Server fakeServer;

  @Mock RpcLogger rpcLogger;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    fakeServer =
        InProcessServerBuilder.forName(fakeServerName)
        .fallbackHandlerRegistry(serviceRegistry)
        .directExecutor()
        .build()
        .start();
  }

  @After
  public void tearDown() throws Exception {
    fakeServer.shutdownNow();
  }

  private void matchStatuses(Status status1, Status status2) {
    assertThat(status1.getCode()).isEqualTo(status2.getCode());
    assertThat(status1.getDescription()).matches(status2.getDescription());
  }

  private ClientInterceptor attachMetadataInterceptor(RequestMetadata meta) {
    Metadata headers = new Metadata();
    headers.put(METADATA_KEY, meta);
    return MetadataUtils.newAttachHeadersInterceptor(headers);
  }

  @Test
  public void testCallOk() {
    GetActionResultRequest request =
        GetActionResultRequest.newBuilder().setInstanceName("test").build();
    ActionResult response = ActionResult.newBuilder().setExitCode(0).build();
    RequestMetadata meta = RequestMetadata.newBuilder().setActionId("test").build();

    serviceRegistry.addService(new ActionCacheImplBase() {
      @Override
      public void getActionResult(
          GetActionResultRequest request, StreamObserver<ActionResult> responseObserver) {
        responseObserver.onNext(response);
        responseObserver.onCompleted();
      }
    });

    LoggingInterceptor loggingInterceptor = new LoggingInterceptor(rpcLogger);
    Channel channel = InProcessChannelBuilder.forName(fakeServerName).directExecutor().build();
    ActionCacheBlockingStub stub =
        ActionCacheGrpc.newBlockingStub(channel)
        .withInterceptors(loggingInterceptor, attachMetadataInterceptor(meta));
    stub.getActionResult(request);

    verify(rpcLogger).logStart(any(), contains("GetActionResult"), eq(meta));
    verify(rpcLogger).logReq(any(), contains("GetActionResult"), eq(request));
    verify(rpcLogger).logResp(any(), contains("GetActionResult"), eq(response));
    verify(rpcLogger).logClose(any(), contains("GetActionResult"), eq(Status.OK));
  }

  @Test
  public void testCallOkWithMultipleResponses() {
    ReadRequest request = ReadRequest.newBuilder().setResourceName("test").build();
    ReadResponse response1 =
        ReadResponse.newBuilder().setData(ByteString.copyFromUtf8("abc")).build();
    ReadResponse response2 =
        ReadResponse.newBuilder().setData(ByteString.copyFromUtf8("def")).build();
    serviceRegistry.addService(new ByteStreamImplBase() {
      @Override
      public void read(
          ReadRequest request, StreamObserver<ReadResponse> responseObserver) {
        responseObserver.onNext(response1);
        responseObserver.onNext(response2);
        responseObserver.onCompleted();
      }
    });

    LoggingInterceptor loggingInterceptor = new LoggingInterceptor(rpcLogger);
    Channel channel = InProcessChannelBuilder.forName(fakeServerName).directExecutor().build();
    ByteStreamBlockingStub stub =
        ByteStreamGrpc.newBlockingStub(channel).withInterceptors(loggingInterceptor);
    Iterator<ReadResponse> responseIterator = stub.read(request);
    advance(responseIterator, 2);

    ArgumentCaptor<ReadResponse> resultCaptor = ArgumentCaptor.forClass(ReadResponse.class);
    verify(rpcLogger).logReq(any(), contains("Read"), eq(request));
    verify(rpcLogger, times(2)).logResp(any(), contains("Read"), resultCaptor.capture());
    assertThat(resultCaptor.getAllValues().get(0).getData().toStringUtf8()).isEqualTo("abc");
    assertThat(resultCaptor.getAllValues().get(1).getData().toStringUtf8()).isEqualTo("def");
    verify(rpcLogger).logClose(any(), contains("Read"), eq(Status.OK));
  }

  @Test
  public void testCallWithNotFoundError() {
    GetActionResultRequest request =
        GetActionResultRequest.newBuilder().setInstanceName("test").build();
    Status error = Status.NOT_FOUND.withDescription("TEST");
    serviceRegistry.addService(new ActionCacheImplBase() {
      @Override
      public void getActionResult(
          GetActionResultRequest request, StreamObserver<ActionResult> responseObserver) {
        responseObserver.onError(error.asRuntimeException());
      }
    });
    LoggingInterceptor loggingInterceptor = new LoggingInterceptor(rpcLogger);
    Channel channel = InProcessChannelBuilder.forName(fakeServerName).directExecutor().build();
    ActionCacheBlockingStub stub =
        ActionCacheGrpc.newBlockingStub(channel)
            .withInterceptors(loggingInterceptor);

    assertThrows(StatusRuntimeException.class, () -> stub.getActionResult(request));
    verify(rpcLogger).logReq(any(), contains("GetActionResult"), eq(request));

    ArgumentCaptor<Status> statusCaptor = ArgumentCaptor.forClass(Status.class);
    verify(rpcLogger).logClose(any(), contains("GetActionResult"), statusCaptor.capture());
    assertThat(statusCaptor.getValue().getCode()).isEqualTo(Status.Code.NOT_FOUND);
    assertThat(statusCaptor.getValue().getDescription()).matches("TEST");
    matchStatuses(statusCaptor.getValue(), error);

    ArgumentCaptor<StatusRuntimeException> exceptionCaptor =
        ArgumentCaptor.forClass(StatusRuntimeException.class);
    verify(rpcLogger).logCancel(any(), contains("GetActionResult"), eq(null),
        exceptionCaptor.capture());
    matchStatuses(exceptionCaptor.getValue().getStatus(), error);

  }
}
