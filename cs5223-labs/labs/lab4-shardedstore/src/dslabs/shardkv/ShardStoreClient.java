package dslabs.shardkv;

import com.google.common.base.Objects;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Result;
import dslabs.kvstore.KVStore.SingleKeyCommand;
import dslabs.paxos.PaxosReply;
import dslabs.paxos.PaxosRequest;
import dslabs.shardkv.ShardStoreServer.reconfig;
import dslabs.shardmaster.ShardMaster;
import dslabs.shardmaster.ShardMaster.Leave;
import dslabs.shardmaster.ShardMaster.Move;
import dslabs.shardmaster.ShardMaster.Query;
import dslabs.shardmaster.ShardMaster.ShardConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ShardStoreClient extends ShardStoreNode implements Client {
  // Your code here...
  public AMOCommand comm;
  private AMOResult res;
  int sequenceNum=0;//initialize
  private PaxosRequest req;
  HashMap<Address,Integer> map2=new HashMap< Address,Integer>();//record (Address,seqNum)
  ShardConfig shardConfig;
  boolean tryNext=false;

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public ShardStoreClient(Address address, Address[] shardMasters, int numShards) {
    super(address, shardMasters, numShards);
  }

  @Override
  public synchronized void init() {
    // Your code here...
    checkIn();
    set(new CheckInTimer(),CheckInTimer.RERTY_MILLIS);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Client Methods
   * ---------------------------------------------------------------------------------------------*/
  @Override
  public synchronized void sendCommand(Command command) {
    // Your code here...
    sequenceNum++;
    Set<Address> servers=findServers(command);
    this.comm= new AMOCommand(command,sequenceNum,this.address());
    if(Objects.equal(servers,null)){
      checkIn();
    }
    else{
      for(Address add:servers){
        send(new ShardStoreRequest(comm),add);
      }
    }
    set(new ClientTimer(comm),ClientTimer.CLIENT_RETRY_MILLIS);
  }

  @Override
  public synchronized boolean hasResult() {
    // Your code here...
    return Objects.equal(map2.get(this.address()),AMOCommand.getSequenceNum(comm));
  }

  @Override
  public synchronized Result getResult() throws InterruptedException {
    // Your code here...
    while (!hasResult()) {
      this.wait();
    }
    return AMOResult.getResult(res);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void handleShardStoreReply(ShardStoreReply m, Address sender) {
    // Your code here...
    if(Objects.equal(AMOResult.getSequenceNum(m.result()),sequenceNum)&&!m.isTrue()){
      checkIn();
      tryNext=true;
    }
    else{
      res = m.result();
      if (Objects.equal(map2.get(AMOResult.getAddress(res)),AMOResult.getSequenceNum(res)))return;
      map2.put(AMOResult.getAddress(res),AMOResult.getSequenceNum(res));
      this.notify();
    }

  }

  // Your code here...
  private void handlePaxosReply(PaxosReply m,Address sender){
    //from shardMaster
    if(m.result()instanceof ShardMaster.Error){
      checkIn();
    }
    else{
//      if(tryNext){
//        if(Objects.equal((ShardConfig)m.result(),shardConfig)){//retry
//          checkIn();
//          return;
//        }
//      }
//      tryNext=false;
      shardConfig= (ShardConfig) m.result();
      //sendCommand(AMOCommand.getCommand(comm));
      if(!Objects.equal(comm,null)){
        if(map2.containsKey(AMOCommand.getAddress(comm))&& (map2.get(AMOCommand.getAddress(comm))>=AMOCommand.getSequenceNum(comm)))return;
        Set<Address> servers=findServers(AMOCommand.getCommand(comm));
        if(Objects.equal(servers,null)){
          Logger.getLogger("").info(this.address()+" servers not found, command: "+comm);
        }
        for(Address add:servers){
          send(new ShardStoreRequest(comm),add);
        }
      }
    }
  }
  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void onClientTimer(ClientTimer t) {
    // Your code here...
    AMOCommand comm= (AMOCommand) t.command();
    Set<Address> servers=findServers(AMOCommand.getCommand(comm));
    if(Objects.equal(servers,null)){
      checkIn();
      Logger.getLogger("").info(this.address()+" servers not found, command: "+comm);
    }
    else{
      if(map2.containsKey(AMOCommand.getAddress(comm))&& (map2.get(AMOCommand.getAddress(comm))>=AMOCommand.getSequenceNum(comm)))return;
      for(Address add:servers){
        send(new ShardStoreRequest(t.command()),add);
      }
    }
    set(t,ClientTimer.CLIENT_RETRY_MILLIS);
  }

  private void onCheckInTimer(CheckInTimer t){
    //periodically send query to shardMaster
    checkIn();
    set(t,CheckInTimer.RERTY_MILLIS);
  }
  private void checkIn(){
    Query query=new Query(-1);//ask shardMaster current shards configuration
    for(Address add:this.shardMasters()){
      send(new PaxosRequest(query),add);
    }
  }
  private Set<Address> findServers(Command command){
    if(Objects.equal(shardConfig,null)){// groupId -> <group members, shard numbers>
      return null;
    }
    SingleKeyCommand singleKeyCommand = (SingleKeyCommand) command;
    String key = singleKeyCommand.key();
    int theShard=keyToShard(key);
    Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo=shardConfig.groupInfo();

    for(Pair<Set<Address>, Set<Integer>> pairs:groupInfo.values()){
      if(pairs.getRight().contains(theShard)){
        return pairs.getLeft();
      }
    }
    return null;
  }
}
