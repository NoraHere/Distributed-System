package dslabs.shardkv;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Command;
import dslabs.framework.Message;
import dslabs.kvstore.KVStore;
import dslabs.shardmaster.ShardMaster.Query;
import dslabs.shardmaster.ShardMaster.ShardConfig;
import java.util.HashMap;
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