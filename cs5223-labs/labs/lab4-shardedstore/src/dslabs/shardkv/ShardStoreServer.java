package dslabs.shardkv;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Command;
import dslabs.kvstore.KVStore;
import dslabs.kvstore.KVStore.SingleKeyCommand;
import dslabs.paxos.PaxosReply;
import dslabs.paxos.PaxosRequest;
import dslabs.paxos.PaxosServer;
import dslabs.shardmaster.ShardMaster;
import dslabs.shardmaster.ShardMaster.Query;
import dslabs.shardmaster.ShardMaster.ShardConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ShardStoreServer extends ShardStoreNode {
  private final Address[] group;
  private final int groupId;

  // Your code here...
  private static final String PAXOS_ADDRESS_ID = "paxos";
  private Address paxosAddress;
  private KVStore kvStore=new KVStore();
  private AMOApplication<KVStore> amoApplication=new AMOApplication<>(kvStore);
  private AMOApplication<KVStore> sendamoApplication;
  private AMOResult res;
  private Address[] shardMasters;
  private int currentConfigNum= ShardMaster.INITIAL_CONFIG_NUM-1;//current config number
  private int myReConfigNum;
  private int sendReConfigNum;
  private Set<Integer> shards;//corresponding shards
  private HashMap<Address,Boolean> Ack_record;
  private int seqNum=0;//reConfig seqNum
  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  ShardStoreServer(
      Address address, Address[] shardMasters, int numShards, Address[] group, int groupId) {
    super(address, shardMasters, numShards);
    this.group = group;
    this.groupId = groupId;

    // Your code here...
    this.kvStore=new KVStore();
    this.shardMasters=shardMasters;
  }

  @Override
  public void init() {
    // Your code here...
    // Setup Paxos
    paxosAddress = Address.subAddress(address(), PAXOS_ADDRESS_ID);

    Address[] paxosAddresses = new Address[group.length];
    for (int i = 0; i < paxosAddresses.length; i++) {
      paxosAddresses[i] = Address.subAddress(group[i], PAXOS_ADDRESS_ID);
    }

    PaxosServer paxosServer = new PaxosServer(
        paxosAddress, paxosAddresses, address());
    addSubNode(paxosServer);
    paxosServer.init();

    //ask for current shards configuration
    Query query=new Query(-1);
    for(Address add:this.shardMasters){
      send(new PaxosRequest(query),add);
    }
    set(new CheckInTimer(),CheckInTimer.RERTY_MILLIS);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void handleShardStoreRequest(ShardStoreRequest m, Address sender) {
    // Your code here...
    if(!Objects.equals(shards,null)){//already received first reply from shardMaster
      AMOCommand comm=(AMOCommand) m.command();
      SingleKeyCommand singleKeyCommand = (SingleKeyCommand) AMOCommand.getCommand(comm);
      String key = singleKeyCommand.key();
      int theShard=keyToShard(key);
      if(!shards.contains(theShard)){//not responsible for this request
        send(new ShardStoreReply(res,false),sender);
      }
      else{//send to local paxos
        handleMessage(new PaxosRequest(m.command()),paxosAddress);
        //Nodes within the same root node can pass messages
      }
    }
  }

  // Your code here...
  private void handlePaxosReply(PaxosReply m,Address sender){
    //from shardMaster
    if(m.result()instanceof ShardMaster.Error){
      //wait
      return;
    }
    else{
      ShardConfig shardConfig= (ShardConfig) m.result();
      if(!Objects.equals(currentConfigNum,shardConfig.configNum())){//send reconfig to local paxos
        seqNum++;
        reconfig reconfig=new reconfig(shardConfig);//command
        AMOCommand comm=new AMOCommand(reconfig,seqNum,this.address());
        handleMessage(new PaxosRequest(comm),paxosAddress);
        currentConfigNum=shardConfig.configNum();//current known configNum
      }
    }
  }
  private void handlePaxosRequest(PaxosRequest m,Address sender){
    //this is reply from local paxos
    AMOCommand comm=(AMOCommand) m.command();
    if(AMOCommand.getCommand(comm) instanceof ShardStoreServer.reconfig){//reconfig
      reconfig reconfig= (ShardStoreServer.reconfig) AMOCommand.getCommand(comm);
      reconfiguration(reconfig.shardConfig);
    }
    else{
      res=amoApplication.execute(m.command());
      send(new ShardStoreReply(res,true),AMOResult.getAddress(res));//send back to client
    }
  }
  private void handleACKReconfig(ACKReconfig m,Address sender){
    if(m.succeed()&&Objects.equals(m.shardConfig().configNum(),sendReConfigNum)){
      Ack_record.put(sender,true);
    }
    if(!Ack_record.containsValue(false)){//all ack
      shards=m.shardConfig().groupInfo().get(this.groupId).getRight();//update shards
    }
  }
  private void handleTransferConfig(TransferConfig m,Address sender){
    if(myReConfigNum>=m.shardConfig().configNum()){//old message
      send(new ACKReconfig(true,m.shardConfig()),sender);
      return;
    }

    if(m.shardConfig().configNum()>currentConfigNum){//first need to get the latest config from shardMaster
      Query query=new Query(-1);//ask shardMaster current shards configuration
      for(Address add:this.shardMasters){
        send(new PaxosRequest(query),add);
      }
    }
    else{
      amoApplication=m.amoApplication();
      myReConfigNum=m.shardConfig().configNum();
      send(new ACKReconfig(true,m.shardConfig()),sender);
    }
  }
  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  // Your code here...
  private void onCheckInTimer(CheckInTimer t){
    //periodically send query to shardMaster
    Query query=new Query(-1);//ask shardMaster current shards configuration
    for(Address add:this.shardMasters){
      send(new PaxosRequest(query),add);
    }
    set(t,CheckInTimer.RERTY_MILLIS);
  }
  private void onTransferConfigTimer(TransferConfigTimer t){
    if(!Ack_record.containsValue(false)){//all ack
      Ack_record.clear();
      return;
    }
    for(Address add:Ack_record.keySet()){
      if(!Ack_record.get(add)){
        send(new TransferConfig(t.shardConfig(),amoApplication),add);
      }
    }
    set(t,TransferConfigTimer.RERTY_MILLIS);
  }
  /* -----------------------------------------------------------------------------------------------
   *  Utils
   * ---------------------------------------------------------------------------------------------*/
  // Your code here...

  private void reconfiguration(ShardConfig shardConfig){
    Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo=shardConfig.groupInfo();
    // groupId -> <group members, shard numbers>
    Pair<Set<Address>, Set<Integer>> pair= groupInfo.get(this.groupId);
    if(Objects.equals(pair,null)){//not involve anymore
      if(Objects.equals(shards, null)) {//initial
        return;
      }
      else{//reconfig
        transferConfig(shardConfig);
      }
    }
    if(!Objects.equals(shards,pair.getRight())){//changes
      if (Objects.equals(shards, null)) {//initial
        shards = pair.getRight();
      }
      else{//reconfig
        transferConfig(shardConfig);
      }
    }
  }

  private void transferConfig(ShardConfig shardConfig){
    Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo=shardConfig.groupInfo();
    sendamoApplication=amoApplication;
    for(Integer num:shards) {
      groupInfo.remove(this.groupId);
      for(Pair<Set<Address>, Set<Integer>> pairs:groupInfo.values()){
        if(pairs.getRight().contains(num)){
          for(Address add:pairs.getLeft()){
            send(new TransferConfig(shardConfig,sendamoApplication),add);
            Ack_record.put(add,false);
          }
        }
      }
    }
    set(new TransferConfigTimer(shardConfig),TransferConfigTimer.RERTY_MILLIS);
    sendReConfigNum=shardConfig.configNum();
  }

  public interface reconfigurationCommand extends Command {}
  @Data
  public static final class reconfig implements reconfigurationCommand {
    private final ShardConfig shardConfig;
  }
}
