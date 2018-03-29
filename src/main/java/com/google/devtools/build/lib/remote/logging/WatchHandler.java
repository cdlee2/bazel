package com.google.devtools.build.lib.remote.logging;

import com.google.devtools.build.lib.remote.logging.RemoteExecutionLog.RpcCallDetails;
import com.google.devtools.build.lib.remote.logging.RemoteExecutionLog.RpcCallDetails.WatchDetails;
import com.google.watcher.v1.ChangeBatch;
import com.google.watcher.v1.Request;

public class WatchHandler implements LoggingHandler<Request, ChangeBatch>{
  private final WatchDetails.Builder builder;

  public WatchHandler() {
    builder = WatchDetails.newBuilder();
  }

  @Override
  public void handleReq(Request message) {
    builder.setRequest(message);
  }

  @Override
  public void handleResp(ChangeBatch message) {
    builder.addChangeBatches(message);
  }

  @Override
  public RpcCallDetails getDetails() {
    return RpcCallDetails.newBuilder().setWatch(builder).build();
  }
}
