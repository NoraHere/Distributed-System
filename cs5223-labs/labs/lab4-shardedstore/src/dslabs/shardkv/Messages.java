package dslabs.shardkv;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Command;
import dslabs.framework.Message;
import dslabs.framework.Result;
import dslabs.kvstore.KVStore;
import dslabs.kvstore.TransactionalKVStore.Transaction;
import dslabs.shardmaster.ShardMaster.Query;
import dslabs.shardmaster.ShardMaster.ShardConfig;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;

@Data
final class ShardStoreRequest implements Message {
  // Your code here...
  private final Command command;
  private final int shardNum;
}

@Data
final class ShardStoreReply implements Message {
  // Your code here...
  private final AMOResult result;
  private final boolean isTrue;
  private final int shardNum;
}

// Your code here...
@Data
final class TransferConfig implements Message{
  //private final HashMap<Integer,Integer> record;
  private final ShardConfig shardConfig;
  private final AMOApplication<?> amoApplication;
  private final int theShard;
}
@Data
final class ACKReconfig implements Message{
  private final boolean succeed;//true is successful received transfer
  private final ShardConfig shardConfig;
  private final int theShard;
}
@Data
final class PrepareMessage implements Message{
  private final AMOCommand comm;//KVStoreCommand
  private final int tryTimes;//tryTimes
  private final int shardConfigNum;
}
@Data
final class PrepareReply implements Message{
  private final boolean vote;//KVStoreCommand
  private final AMOCommand comm;//KVStoreCommand
  private final int groupId;
  private final int tryTimes;//tryTimes
  private final int shardConfigNum;
  private final Map<String, String> res;//read data
}
@Data
final class DecisionMessage implements Message{
  private final boolean decision;//commit/abort
  private final int tryTimes;//tryTimes
  private final AMOCommand comm;//KVStoreCommand
  private final int shardConfigNum;
}
@Data
final class AskDecisionMessage implements Message{
  private final AMOCommand comm;//KVStoreCommand
  private final int tryTime;
  private final int shardConfigNum;
}