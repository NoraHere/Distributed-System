package dslabs.shardkv;

import com.google.common.base.Objects;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Result;
import dslabs.kvstore.KVStore.SingleKeyCommand;
import dslabs.kvstore.TransactionalKVStore.Transaction;
import dslabs.paxos.PaxosReply;
import dslabs.paxos.PaxosRequest;
import dslabs.shardkv.ShardStoreServer.reconfig;
import dslabs.shardmaster.ShardMaster;
import dslabs.shardmaster.ShardMaster.Leave;
import dslabs.shardmaster.ShardMaster.Move;
import dslabs.shardmaster.ShardMaster.Query;
import dslabs.shardmaster.ShardMaster.ShardConfig;
import java.util.HashMap;
import java.util.HashSet;
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
    Set<Address> servers;
    if(command instanceof SingleKeyCommand){
      servers = findServers(command);
    }
    else {//part3
      //decide the coordinator
      servers=findCoordinator(command);
    }

    this.comm= new AMOCommand(command,sequenceNum,this.address());
    if(Objects.equal(servers,null)){
      checkIn();
    }
    else{
      for(Address add:servers){
        send(new ShardStoreRequest(comm,shardConfig.configNum()),add);
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
    if(Objects.equal(AMOResult.getSequenceNum(m.result()),sequenceNum)){
      if(!m.isTrue()){
        if(m.shardNum()<shardConfig.configNum()){
          //resend
        }
        else if(m.shardNum()>shardConfig.configNum()){
          checkIn();
          tryNext=true;
        }
        else{
          //error
          Logger.getLogger("").info("ERROR");
        }

      }
      else{
        res = m.result();
        if (Objects.equal(map2.get(AMOResult.getAddress(res)),AMOResult.getSequenceNum(res)))return;
        map2.put(AMOResult.getAddress(res),AMOResult.getSequenceNum(res));
        this.notify();
      }
    }
  }

  // Your code here...
  private void handlePaxosReply(PaxosReply m,Address sender){
    //from shardMaster
    if(m.result()instanceof ShardMaster.Error){
      checkIn();
    }
    else{
      if(tryNext){
        if(((ShardConfig)m.result()).configNum()<=shardConfig.configNum()){//retry
          checkIn();
          return;
        }
      }
      tryNext=false;

      ShardConfig mayshardConfig= (ShardConfig) m.result();
      if(Objects.equal(shardConfig,null)||(!Objects.equal(mayshardConfig,null)&&mayshardConfig.configNum()>shardConfig.configNum())){
        shardConfig=mayshardConfig;
      }
      if(!Objects.equal(comm,null)){
        if(map2.containsKey(AMOCommand.getAddress(comm))&& (map2.get(AMOCommand.getAddress(comm))>=AMOCommand.getSequenceNum(comm)))return;
        Set<Address> servers;
        if(comm.command() instanceof SingleKeyCommand){
          servers = findServers(comm.command());
        }
        else {//part3
          servers=findCoordinator(comm.command());//decide the coordinator
        }
        if(Objects.equal(servers,null)){
          Logger.getLogger("").info(this.address()+" servers not found, command: "+comm);
        }
        else{
          for(Address add:servers){
            send(new ShardStoreRequest(comm,shardConfig.configNum()),add);
          }
        }

      }
    }
  }
  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void onClientTimer(ClientTimer t) {
    // Your code here...
    Command command= t.command().command();
    Set<Address> servers;
    if(command instanceof SingleKeyCommand){
      servers = findServers(command);
    }
    else {//part3
      //decide the coordinator
      servers=findCoordinator(command);
    }

    AMOCommand comm= t.command();
    if(Objects.equal(servers,null)){
      checkIn();
      Logger.getLogger("").info(this.address()+" servers not found, command: "+comm);
    }
    else{
      if(map2.containsKey(AMOCommand.getAddress(comm))&& (map2.get(AMOCommand.getAddress(comm))>=AMOCommand.getSequenceNum(comm)))return;
      for(Address add:servers){
        send(new ShardStoreRequest(comm,shardConfig.configNum()),add);
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
    return null;//no found, should not happen
  }
  private HashMap<Integer,Set<Address>> findTransactionServers(Command command){//part3
    if(com.google.common.base.Objects.equal(shardConfig,null)){// groupId -> <group members, shard numbers>
      return null;
    }
    Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo=shardConfig.groupInfo();
    if(command instanceof Transaction) {//transactional requests, part3
      // groupId -> <group members, shard numbers>
      HashMap<Integer,Set<Address>> servers = new HashMap<>();//groupId:servers
      for (String key : ((Transaction) command).keySet()) {
        int theShard = keyToShard(key);
        for(Integer groupId: groupInfo.keySet()){
          if(!java.util.Objects.equals(groupInfo.get(groupId).getRight(),null)&&groupInfo.get(groupId).getRight().contains(theShard)){
            servers.put(groupId,groupInfo.get(groupId).getLeft());
          }
        }
      }
      return servers;
    }
    return null;//only deal with transaction
  }
  private Set<Address> findCoordinator(Command command){//part3
    HashMap<Integer,Set<Address>> servers=findTransactionServers(command);
    if(Objects.equal(servers,null)){
      return null;
    }
    else{// groupId -> <group members, shard numbers>
      int theMaxId=0;
      for(Integer id:servers.keySet()){
        if(id>theMaxId){
          theMaxId=id;
        }
      }
      return servers.get(theMaxId);
    }
  }
}
