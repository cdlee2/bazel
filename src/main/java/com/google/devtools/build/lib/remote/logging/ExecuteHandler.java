package com.google.devtools.build.lib.remote.logging;


import com.google.devtools.build.lib.buildeventstream.BuildEventStreamProtos.RpcCallDetails;
import com.google.devtools.build.lib.buildeventstream.BuildEventStreamProtos.RpcCallDetails.ExecuteDetails;
import com.google.devtools.remoteexecution.v1test.ExecuteRequest;
import com.google.longrunning.Operation;

public class ExecuteHandler implements LoggingHandler<ExecuteRequest, Operation> {

  private ExecuteDetails.Builder detailsBuilder;

  public ExecuteHandler() {
    detailsBuilder = ExecuteDetails.newBuilder();
  }

  @Override
  public void handleReq(ExecuteRequest message) {
    detailsBuilder.setRequest(message);
  }

  @Override
  public void handleResp(Operation message) {
    detailsBuilder.setOperation(message);
  }

  @Override
  public RpcCallDetails getDetails() {
    return RpcCallDetails.newBuilder().setExecute(detailsBuilder).build();
  }
}
