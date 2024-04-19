package dslabs.shardkv;

import dslabs.atmostonce.AMOApplication;
import dslabs.framework.Command;
import dslabs.framework.Timer;
import dslabs.shardmaster.ShardMaster.Query;
import dslabs.shardmaster.ShardMaster.ShardConfig;
import lombok.Data;

@Data
final class ClientTimer implements Timer {
  static final int CLIENT_RETRY_MILLIS = 100;

  // Your code here...
  private final Command command;
}

// Your code here...
@Data
final class CheckInTimer implements Timer{
  static final int RERTY_MILLIS=30;
}
@Data
final class TransferConfigTimer implements Timer{
  static final int RERTY_MILLIS=100;
  private final ShardConfig shardConfig;
  private final AMOApplication<?> sendamoApplication;
}