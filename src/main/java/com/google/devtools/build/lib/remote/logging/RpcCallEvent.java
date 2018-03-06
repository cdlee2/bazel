package com.google.devtools.build.lib.remote.logging;

import com.google.common.collect.ImmutableList;
import com.google.devtools.build.lib.buildeventstream.BuildEvent;
import com.google.devtools.build.lib.buildeventstream.BuildEventConverters;
import com.google.devtools.build.lib.buildeventstream.BuildEventId;
import com.google.devtools.build.lib.buildeventstream.BuildEventStreamProtos;
import com.google.devtools.build.lib.buildeventstream.BuildEventStreamProtos.RpcCall;
import com.google.devtools.build.lib.buildeventstream.BuildEventStreamProtos.RpcCallDetails;
import com.google.devtools.build.lib.buildeventstream.GenericBuildEvent;
import com.google.devtools.remoteexecution.v1test.RequestMetadata;
import io.grpc.Status;
import java.util.Collection;
import javax.annotation.Nullable;

public class RpcCallEvent implements BuildEvent {

  private final RequestMetadata metadata;
  private final String methodName;
  private final Status status;
  private final RpcCallDetails details;

  public RpcCallEvent(
      String methodName,
      @Nullable RequestMetadata metadata,
      Status status,
      RpcCallDetails details) {
    this.methodName = methodName;
    this.metadata = metadata;
    this.status = status;
    this.details = details;
  }

  @Override
  public BuildEventStreamProtos.BuildEvent asStreamProto(BuildEventConverters converters) {
    RpcCall.Builder eventBuilder = RpcCall.newBuilder()
        .setMethodName(methodName)
        .setStatus(makeStatusProto(status))
        .setDetails(details);
    if (metadata != null) {
      eventBuilder.setMetadata(metadata);
    }

    return GenericBuildEvent.protoChaining(this).setRpcCall(eventBuilder.build()).build();
  }

  @Override
  public BuildEventId getEventId() {
    return BuildEventId.rpcCall();
  }

  @Override
  public Collection<BuildEventId> getChildrenEvents() {
    return ImmutableList.<BuildEventId>of();
  }

  /** Converts io.grpc.Status to com.google.rpc.Status proto for logging. */
  private static com.google.rpc.Status makeStatusProto(Status status) {
    String message = "";
    if (status.getCause() != null) {
      message = status.getCause().toString();
    } else if (status.getDescription() != null) {
      message = status.getDescription();
    }
    return com.google.rpc.Status.newBuilder()
        .setCode(status.getCode().value())
        .setMessage(message)
        .build();
  }
}
