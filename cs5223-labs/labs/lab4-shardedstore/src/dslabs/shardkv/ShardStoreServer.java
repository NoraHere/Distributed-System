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
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Logger;
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
  private AMOApplication<?> amoApplication=new AMOApplication<>(kvStore);
  private AMOApplication<?> sendamoApplication;
  private HashMap<Integer,AMOApplication<?>>amoApplication_records;//shards:amo
  private AMOResult res;
  private Address[] shardMasters;
  private int nextConfigNum= ShardMaster.INITIAL_CONFIG_NUM;//ask current config number(query)
  private int ackReConfigNum;//configNum received all ack
  private Set<Integer> shards;//corresponding shards
  private HashMap<Integer,HashMap<Address,Boolean>> Ack_record=new HashMap<>();//configNum:<Add:true/false>
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
    checkIn();
    set(new CheckInTimer(),CheckInTimer.RERTY_MILLIS);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void handleShardStoreRequest(ShardStoreRequest m, Address sender) {
    // Your code here...
    //from client
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
      checkIn();//resend
    }
    else{
      ShardConfig shardConfig= (ShardConfig) m.result();
      if(Objects.equals(nextConfigNum,ShardMaster.INITIAL_CONFIG_NUM)){//initial state
        if(!Objects.equals(((ShardConfig) m.result()).groupInfo().get(groupId),null)){
          shards=((ShardConfig) m.result()).groupInfo().get(groupId).getRight();
        }
        ackReConfigNum=ShardMaster.INITIAL_CONFIG_NUM;
        nextConfigNum++;
      }
      if(shardConfig.configNum()<=nextConfigNum-1){//old messages
        return;
      }
      if(nextConfigNum-1<shardConfig.configNum()){//shardMaster has changes
        seqNum++;
        reconfig reconfig=new reconfig(shardConfig);//command
        AMOCommand comm=new AMOCommand(reconfig,seqNum,this.address());
        handleMessage(new PaxosRequest(comm),paxosAddress);
        //make sure this reconfig execute and then next reconfig???
      }
    }
  }
  private void handlePaxosRequest(PaxosRequest m,Address sender){
    //this is reply from local paxos: next operation to execute
    AMOCommand comm=(AMOCommand) m.command();
    if(AMOCommand.getCommand(comm) instanceof ShardStoreServer.reconfig){//reconfig
      reconfig newreconfig= (ShardStoreServer.reconfig) AMOCommand.getCommand(comm);
      reconfiguration(newreconfig.shardConfig);
    }
    else if (AMOCommand.getCommand(comm) instanceof ShardStoreServer.ackReconfig) {
      ackReconfig ack=((ackReconfig) AMOCommand.getCommand(comm));
      int mayAckReConfigNum=ack.shardConfig.configNum();//record current ackReConfigNum
      if(mayAckReConfigNum>ackReConfigNum&&Objects.equals(groupId,ack.groupId)){
        ackReConfigNum=mayAckReConfigNum;
        nextConfigNum++;//can move to next query
        Ack_record.remove(mayAckReConfigNum);
        Logger.getLogger("").info("successful ack: " + ackReConfigNum);
      }
    } else{
      res=amoApplication.execute(m.command());
      send(new ShardStoreReply(res,true),AMOResult.getAddress(res));//send back to client
    }
  }
  private void handleACKReconfig(ACKReconfig m,Address sender){
    HashMap<Address,Boolean> current_record=Ack_record.get(m.shardConfig().configNum());
    if(m.succeed()&&Ack_record.containsKey(m.shardConfig().configNum())){
      current_record.put(sender,true);
    }
    if(!Objects.equals(current_record,null)&&!current_record.containsValue(false)){//current record all ack
      //successfully transfer
      //groupId -> <group members, shard numbers>
      Pair<Set<Address>, Set<Integer>> mayInfo=m.shardConfig().groupInfo().get(this.groupId);
      if(!Objects.equals(mayInfo,null)){
        shards=mayInfo.getRight();//update shards
      }
      ackReconfig ack=new ackReconfig(m.shardConfig(),groupId);
      seqNum++;
      handleMessage(new PaxosRequest(new AMOCommand(ack,seqNum,this.address())),paxosAddress);//consensus successful transfer
    }
  }
  private void handleTransferConfig(TransferConfig m,Address sender){
    if(nextConfigNum-1>m.shardConfig().configNum()){//old message
      send(new ACKReconfig(true,m.shardConfig()),sender);
      return;
    }
    else if(m.shardConfig().configNum()>nextConfigNum-1){//first need to get the latest config from shardMaster
      checkIn();//ensure my shards have already sent
    }
    else{//nextConfigNum-1==m.shardConfig.configNum
      //m.amoApplication().map3().;
      amoApplication=new AMOApplication<>(m.amoApplication());//receive
      Logger.getLogger("").info("received amoapplication from transfer " +this.address()+" "+ amoApplication);
      send(new ACKReconfig(true,m.shardConfig()),sender);
    }
  }
  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  // Your code here...
  private void onCheckInTimer(CheckInTimer t){
    //periodically send query to shardMaster
    checkIn();
    set(t,CheckInTimer.RERTY_MILLIS);
  }
  private void onTransferConfigTimer(TransferConfigTimer t){

    if(t.shardConfig().configNum()<=ackReConfigNum){//old transfer
      return;
    }
    for(Address add:Ack_record.get(t.shardConfig().configNum()).keySet()){
      if(!Ack_record.get(t.shardConfig().configNum()).get(add)){//this add is false
        send(new TransferConfig(t.shardConfig(),t.sendamoApplication()),add);
      }
    }
    set(t,TransferConfigTimer.RERTY_MILLIS);
  }
  /* -----------------------------------------------------------------------------------------------
   *  Utils
   * ---------------------------------------------------------------------------------------------*/
  // Your code here...

  private void checkIn(){
    Query query=new Query(nextConfigNum);//ask shardMaster next shards configuration
    for(Address add:this.shardMasters){
      send(new PaxosRequest(query),add);
    }
  }
  private void reconfiguration(ShardConfig shardConfig){
    Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo=shardConfig.groupInfo();
    // groupId -> <group members, shard numbers>
    if(Objects.equals(shards, null)){//no need to do anything
      ackReconfig ack=new ackReconfig(shardConfig,groupId);
      seqNum++;
      handleMessage(new PaxosRequest(new AMOCommand(ack,seqNum,this.address())),paxosAddress);//consensus successful transfer
    }
    else{//shards!=null
      Pair<Set<Address>, Set<Integer>> pair= groupInfo.get(this.groupId);
      if(Objects.equals(pair,null)){//not involve anymore,leave or never show up
        transferConfig(shardConfig);
      }
      else{//pair!=null
        Set<Integer> nextShards=pair.getRight();
        if(!Objects.equals(nextShards,shards)){
          transferConfig(shardConfig);
        }
        else{
          ackReconfig ack=new ackReconfig(shardConfig,groupId);
          seqNum++;
          handleMessage(new PaxosRequest(new AMOCommand(ack,seqNum,this.address())),paxosAddress);//consensus successful transfer
        }
      }
    }

//    Pair<Set<Address>, Set<Integer>> pair= groupInfo.get(this.groupId);
//    if(Objects.equals(pair,null)){//not involve anymore,leave or never show up
//      if(!Objects.equals(shards, null)) {//changes
//        transferConfig(shardConfig);
//      }
//      else{
//        ackReconfig ack=new ackReconfig(shardConfig,groupId);
//        seqNum++;
//        handleMessage(new PaxosRequest(new AMOCommand(ack,seqNum,this.address())));//consensus successful transfer
//      }
//    }
//    else{//pair !=null, will involve
//      Set<Integer> nextShards=pair.getRight();
//      if(Objects.equals(nextShards,null)){//moved
//        if(!Objects.equals(shards, null)){//now has shards
//          transferConfig(shardConfig);
//        }
//        else{
//          ackReconfig ack=new ackReconfig(shardConfig);
//          seqNum++;
//          handleMessage(new PaxosRequest(new AMOCommand(ack,seqNum,this.address())));//consensus successful transfer
//        }
//      }
//      else{//nextShards!=null
//        if(!Objects.equals(shards,nextShards)){//changes
//          if (Objects.equals(shards, null)) {//nearly initial
//            shards = pair.getRight();
//            ackReconfig ack=new ackReconfig(shardConfig);
//            seqNum++;
//            handleMessage(new PaxosRequest(new AMOCommand(ack,seqNum,this.address())));//consensus successful transfer
//          }
//          else{//shard!=null
//            transferConfig(shardConfig);
//          }
//        }
//        else{
//          ackReconfig ack=new ackReconfig(shardConfig);
//          seqNum++;
//          handleMessage(new PaxosRequest(new AMOCommand(ack,seqNum,this.address())));//consensus successful transfer
//        }
//      }
//    }
    //finish reconfiguration
  }

  private void transferConfig(ShardConfig shardConfig){
    //shards!=null
    // groupId -> <group members, shard numbers>
    if(ackReConfigNum>=shardConfig.configNum()){//old messages
      return;
    }
    Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo=shardConfig.groupInfo();
    Map<Integer, Pair<Set<Address>, Set<Integer>>> newgroupInfo = new HashMap<>();//deep copy
    for (Map.Entry<Integer, Pair<Set<Address>, Set<Integer>>> entry : groupInfo.entrySet()) {
      Set<Address> deepCopiedAddresses = new HashSet<>(entry.getValue().getLeft());
      Set<Integer> deepCopiedShards = new HashSet<>(entry.getValue().getRight());
      newgroupInfo.put(entry.getKey(), Pair.of(deepCopiedAddresses, deepCopiedShards));
    }
    newgroupInfo.remove(groupId);
    sendamoApplication=new AMOApplication<>(amoApplication);
    for(Integer num:shards) {//send shards info to new group
      for(Pair<Set<Address>, Set<Integer>> pairs:newgroupInfo.values()){
        if(!Objects.equals(pairs.getRight(),null)&&pairs.getRight().contains(num)){
          HashMap<Address,Boolean> record=new HashMap<>();
          for(Address add:pairs.getLeft()){
            send(new TransferConfig(shardConfig,sendamoApplication),add);
            record.put(add,false);
          }
          Ack_record.put(shardConfig.configNum(),record);
          Logger.getLogger("").info("sendamoapplication: " +this.address()+" "+ sendamoApplication);
          break;
        }
      }
    }
    set(new TransferConfigTimer(shardConfig,sendamoApplication),TransferConfigTimer.RERTY_MILLIS);
  }

  public interface reconfigurationCommand extends Command {}
  @Data
  public static final class reconfig implements reconfigurationCommand {
    private final ShardConfig shardConfig;
  }
  @Data
  public static final class ackReconfig implements reconfigurationCommand {
    //used to reach consensus that transfer config succeed
    private final ShardConfig shardConfig;
    private final int groupId;
  }
}
