package dslabs.shardkv;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Address;
import dslabs.framework.Command;
import dslabs.framework.Timer;
import dslabs.kvstore.TransactionalKVStore.Transaction;
import dslabs.shardmaster.ShardMaster.Query;
import dslabs.shardmaster.ShardMaster.ShardConfig;
import lombok.Data;

@Data
final class ClientTimer implements Timer {
  static final int CLIENT_RETRY_MILLIS = 100;

  // Your code here...
  private final AMOCommand command;
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
//  private final AMOApplication<?> amoApplication;
//  private final int theShard;
}
@Data
final class PrepareMessageTimer implements Timer{
  static final int RERTY_MILLIS=100;
  private final AMOCommand comm;
  private final int tryTimes;//tryTimes
  private final int configNum;
}
@Data
final class DecisionTimer implements Timer{
  static final int RERTY_MILLIS=200;
  private final AMOCommand comm;
  private final int tryTimes;//tryTimes
  private final Address coordinatorAdd;
  private final int configNum;
}
//@Data
//final class CommitMessageTimer implements Timer{
//  static final int RERTY_MILLIS=100;
//  private final AMOCommand comm;
//  private final boolean decision;//yes/abort
//  private final int tryTimes;//tryTimes
//  private final int configNum;
//}