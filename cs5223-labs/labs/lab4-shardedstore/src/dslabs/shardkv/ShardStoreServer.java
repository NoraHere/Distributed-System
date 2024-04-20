package dslabs.shardkv;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Command;
import dslabs.framework.Result;
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
import java.util.Iterator;
import java.util.LinkedList;
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
  //private AMOApplication<?> amoApplication=new AMOApplication<>(kvStore);
  //private AMOApplication<?> sendamoApplication;
  private HashMap<Integer,AMOApplication<?>>amoApplication_records=new HashMap<>();//shards:amo
  private AMOResult res;
  private Address[] shardMasters;
  private int nextConfigNum= ShardMaster.INITIAL_CONFIG_NUM;//ask current config number(query)
  private int ackReConfigNum=-1;//configNum received all ack(successful sent)
  private Set<Integer> shards;//corresponding shards
  private HashMap<Integer,HashMap<Address,Boolean>> Ack_record=new HashMap<>();//shard:<Add:true/false>
  private int seqNum=0;//reConfig seqNum
  private boolean firstTime=true;
  private boolean allReceived=false;//successful received all shards
  LinkedList<Command> commandList = new LinkedList<>();
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
    if(nextConfigNum>ShardMaster.INITIAL_CONFIG_NUM) {//already received first reply from shardMaster
      AMOCommand comm = (AMOCommand) m.command();
      if (!Objects.equals(shards, null)) {
        SingleKeyCommand singleKeyCommand = (SingleKeyCommand) AMOCommand.getCommand(comm);
        String key = singleKeyCommand.key();
        int theShard = keyToShard(key);
        if (!shards.contains(theShard)) {//not responsible for this request
          res = new AMOResult(null, AMOCommand.getSequenceNum(comm), AMOCommand.getAddress(comm));
          send(new ShardStoreReply(res, false), sender);
        } else {//send to local paxos
          handleMessage(new PaxosRequest(m.command()), paxosAddress);
          //Nodes within the same root node can pass messages
        }
      }
      else{
        res=new AMOResult(null,AMOCommand.getSequenceNum(comm),AMOCommand.getAddress(comm));
        send(new ShardStoreReply(res,false),sender);
      }
    }
    else{
      checkIn();
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
      if(shardConfig.configNum()<=nextConfigNum-1||(shardConfig.configNum()<=ackReConfigNum)){//old messages
        return;
      }

      if(Objects.equals(nextConfigNum,ShardMaster.INITIAL_CONFIG_NUM)){//initial state
        if(!Objects.equals(((ShardConfig) m.result()).groupInfo().get(groupId),null)){//if this.groupID involve
          shards=((ShardConfig) m.result()).groupInfo().get(groupId).getRight();
        }
        ackReConfigNum=ShardMaster.INITIAL_CONFIG_NUM;
        nextConfigNum++;
        if(!Objects.equals(shards,null)){//initialize amoApplication_records
          for(Integer shard:shards){
            amoApplication_records.put(shard,new AMOApplication<>(kvStore));
          }
        }
        return;
      }
      else{
        if(nextConfigNum-1<shardConfig.configNum()&&firstTime){//shardMaster has changes
          firstTime=false;
          seqNum++;
          reconfig reconfig=new reconfig(shardConfig);//command
          AMOCommand comm=new AMOCommand(reconfig,seqNum,this.address());
          handleMessage(new PaxosRequest(comm),paxosAddress);
          //make sure this reconfig execute and then next reconfig
        }
      }


    }
  }
  private void handlePaxosRequest(PaxosRequest m,Address sender){
    //this is reply from local paxos: next operation to execute
    commandList.add(m.command());
    AMOCommand comm=(AMOCommand) m.command();
    if(AMOCommand.getCommand(comm) instanceof ShardStoreServer.reconfig){
      reconfig newreconfig= (ShardStoreServer.reconfig) AMOCommand.getCommand(comm);
      reconfiguration(newreconfig.shardConfig);
    }
    else if (AMOCommand.getCommand(comm) instanceof ShardStoreServer.ackReconfig) {//all ack
      ackReconfig ack=((ackReconfig) AMOCommand.getCommand(comm));
      int mayAckReConfigNum=ack.shardConfig.configNum();//record current ackReConfigNum
      if(mayAckReConfigNum>ackReConfigNum&&Objects.equals(groupId,ack.groupId)){
        ackReConfigNum=mayAckReConfigNum;
        Logger.getLogger("").info("new ackReConfigNum: "+ackReConfigNum);
        Ack_record.clear();
        Set<Integer> newshards;
        if(!Objects.equals(ack.shardConfig.groupInfo().get(this.groupId),null)){
          newshards=ack.shardConfig.groupInfo().get(this.groupId).getRight();
        }
        else{
          newshards=null;
        }
        if(allReceived||Objects.equals(newshards,null)){//all received/no need to receive, next query
          Iterator<Integer> iterator = amoApplication_records.keySet().iterator();//update amoApplication_records
          while (iterator.hasNext()) {
            Integer shard = iterator.next();
            if (!shards.contains(shard)) {
              iterator.remove(); // Removes the current key safely
            }
          }
          //Logger.getLogger("").info("amoApplication_records after clearance: "+amoApplication_records);
          shards=newshards;//update shards
          nextConfigNum++;firstTime=true;allReceived=false;
          Logger.getLogger("").info(this.address()+ " move to nextConfigNum: "+ nextConfigNum);
          Logger.getLogger("").info("amoApplictaion_records: "+amoApplication_records);
        }
        //Ack_record.remove(mayAckReConfigNum);
        Logger.getLogger("").info("all successful ack: " + ackReConfigNum);

//        for(Integer shard:amoApplication_records.keySet()){//clear old amoApplication_records error
//          if(!shards.contains(shard)){
//            amoApplication_records.remove(shard);
//          }
//        }
      }
    }
    else{
      int theShard=findTheShard(AMOCommand.getCommand(comm));
      if(Objects.equals(amoApplication_records.get(theShard),null)){//should not happen
        checkIn();
        return;
      }
      res=amoApplication_records.get(theShard).execute(m.command());
      //res=amoApplication.execute(m.command());
      send(new ShardStoreReply(res,true),AMOResult.getAddress(res));//send back to client
    }
  }
  private void handleACKReconfig(ACKReconfig m,Address sender){
    //sender server receive ACKReconfig
    if(m.shardConfig().configNum()<=ackReConfigNum){//old message
      Logger.getLogger("").info("old ACK message, ackReConfigNum: "+ackReConfigNum);
      return;
    }

    HashMap<Address,Boolean> current_record=Ack_record.get(m.theShard());
    if(m.succeed()&&Ack_record.containsKey(m.theShard())){
      current_record.put(sender,true);
    }
    Ack_record.put(m.theShard(),current_record);//update Ack_record
    //Logger.getLogger("").info("Ack_record: "+Ack_record);

    Boolean allACK=true;//check if this shardConfig all successfully transfer
    for(Integer shard:Ack_record.keySet()){
      if(Ack_record.get(shard).containsValue(false)){
        allACK=false;
        break;
      }
    }
    if(allACK){//send ackReconfig command to Paxos
    //}
    //if(!Objects.equals(current_record,null)&&!current_record.containsValue(false)){//current record all ack
      //successfully transfer
      //groupId -> <group members, shard numbers>
//      Pair<Set<Address>, Set<Integer>> mayInfo=m.shardConfig().groupInfo().get(this.groupId);
//      if(!Objects.equals(mayInfo,null)){//update my shards
//        shards=mayInfo.getRight();
//      }
//      else{
//        shards=null;
//      }
      ackReconfig ack=new ackReconfig(m.shardConfig(),groupId);//command
      seqNum++;
      handleMessage(new PaxosRequest(new AMOCommand(ack,seqNum,this.address())),paxosAddress);//consensus successful transfer
      Logger.getLogger("").info("all ack of shardNum: "+ack.shardConfig.configNum());
    }
  }
  private void handleTransferConfig(TransferConfig m,Address sender){
    //new group servers receive
    if(nextConfigNum>m.shardConfig().configNum()||amoApplication_records.containsKey(m.theShard())){//old message
      send(new ACKReconfig(true,m.shardConfig(),m.theShard()),sender);
      Logger.getLogger("").info("old Transfer message, nextReConfigNum: "+nextConfigNum);
    }
    else if(m.shardConfig().configNum()>nextConfigNum){//first need to get the latest config from shardMaster
      checkIn();//ensure my shards have already sent
      Logger.getLogger("").info("checkIn()");
    }
    else{//nextConfigNum-1==m.shardConfig.configNum
      //m.amoApplication().map3().;
      if(m.shardConfig().groupInfo().get(this.groupId).getRight().contains(m.theShard())){//this groupID do need the shard
        amoApplication_records.put(m.theShard(),m.amoApplication());
        //amoApplication=new AMOApplication<>(m.amoApplication());//receive
        Logger.getLogger("").info(this.address()+" received transfer amoapplication of this shard: "+ m.theShard());
        //Logger.getLogger("").info(this.address()+" amoapplication_records: "+ amoApplication_records);
        send(new ACKReconfig(true,m.shardConfig(),m.theShard()),sender);
      }
      else{
        send(new ACKReconfig(false,m.shardConfig(),m.theShard()),sender);//should not happen
        Logger.getLogger("").info("ERROR Shard transfer");
      }
    }
    if(Objects.equals(amoApplication_records.keySet(),m.shardConfig().groupInfo().get(this.groupId).getRight())){//all received
      allReceived=true;
      Logger.getLogger("").info(this.address()+" all received, configNum: "+m.shardConfig().configNum());
    }
    if(allReceived&&Objects.equals(ackReConfigNum,nextConfigNum)){//Received and sent, can move to next query
      shards=m.shardConfig().groupInfo().get(this.groupId).getRight();//update shards
      nextConfigNum++;firstTime=true;allReceived=false;
      Logger.getLogger("").info(this.address()+" move to nextConfigNum: "+ nextConfigNum);
      Logger.getLogger("").info("amoApplictaion_records: "+amoApplication_records);
    }
  }
  /* ---------------------------------------------------------------------------q--------------------
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
    for(Integer shard:Ack_record.keySet()){//re-transfer config
      for(Address add:Ack_record.get(shard).keySet()){
        if(!Ack_record.get(shard).get(add)){
          send(new TransferConfig(t.shardConfig(),amoApplication_records.get(shard),shard),add);
        }
      }
    }
//    for(Address add:Ack_record.get(t.shardConfig().configNum()).keySet()){
//      if(!Ack_record.get(t.shardConfig().configNum()).get(add)){//this add is false
//        transferConfig(t.shardConfig());
//        send(new TransferConfig(t.shardConfig(),t.amoApplication(),t.theShard()),add);
//      }
//    }
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
    if(Objects.equals(shards, null)){//no need to transfer
      Ack_record.clear();
      ackReConfigNum=shardConfig.configNum();
      Logger.getLogger("").info("new ackReConfigNum: "+ackReConfigNum);
      Set<Integer> newshards;
      if(!Objects.equals(shardConfig.groupInfo().get(this.groupId),null)){
        newshards=shardConfig.groupInfo().get(this.groupId).getRight();
      }
      else{
        newshards=null;
      }
      if(Objects.equals(newshards,null)){//no need to receive,can move to next query
        shards=newshards;//update shards
        nextConfigNum++;firstTime=true;allReceived=false;
        Logger.getLogger("").info(this.address()+ " move to nextConfigNum: "+ nextConfigNum);
        Logger.getLogger("").info("amoApplictaion_records: "+amoApplication_records);
      }
      //else wait to receive

      //Ack_record.remove(mayAckReConfigNum);
    }
    else{//shards!=null
      Pair<Set<Address>, Set<Integer>> pair= groupInfo.get(this.groupId);
      if(Objects.equals(pair,null)){//not involve anymore,leave or never show up
        allReceived=true;//no need to received
        transferConfig(shardConfig);
        set(new TransferConfigTimer(shardConfig),TransferConfigTimer.RERTY_MILLIS);
      }
      else{//pair!=null
        Set<Integer> nextShards=pair.getRight();
        if(!Objects.equals(nextShards,shards)){
          boolean noNeedR=true;//no need to receive
          boolean noNeedS=true;//no need to send
          for (Integer shard : nextShards) {
            if (!shards.contains(shard)) {
              noNeedR = false;
              break;
            }
          }
          for (Integer shard : shards) {
            if (!nextShards.contains(shard)) {
              noNeedS = false;
              break;
            }
          }
          if(noNeedR){
            allReceived=true;
          }
          if(noNeedS){
            ackReConfigNum=shardConfig.configNum();
          }
          transferConfig(shardConfig);
          set(new TransferConfigTimer(shardConfig),TransferConfigTimer.RERTY_MILLIS);
        }
        else{//no need to transfer, no need to receive, can move to next query
          ackReConfigNum=shardConfig.configNum();
          Logger.getLogger("").info("new ackReConfigNum: "+ackReConfigNum);
          nextConfigNum++;firstTime=true;allReceived=false;
          Logger.getLogger("").info(this.address()+ " move to nextConfigNum: "+ nextConfigNum);
          Logger.getLogger("").info("amoApplictaion_records: "+amoApplication_records);
          //else wait to receive
        }
      }
    }
  }

  private void transferConfig(ShardConfig shardConfig){
    //shards!=null
    // groupId -> <group members, shard numbers>
    Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo=shardConfig.groupInfo();

    Map<Integer, Pair<Set<Address>, Set<Integer>>> newgroupInfo = new HashMap<>();//deep copy
    for (Map.Entry<Integer, Pair<Set<Address>, Set<Integer>>> entry : groupInfo.entrySet()) {
      Set<Address> deepCopiedAddresses = new HashSet<>(entry.getValue().getLeft());
      Set<Integer> deepCopiedShards = new HashSet<>(entry.getValue().getRight());
      newgroupInfo.put(entry.getKey(), Pair.of(deepCopiedAddresses, deepCopiedShards));
    }

    newgroupInfo.remove(groupId);
    //sendamoApplication=new AMOApplication<>(amoApplication);
    for(Integer num:shards) {//send shards info to new group
      for(Pair<Set<Address>, Set<Integer>> pairs:newgroupInfo.values()){
        if(!Objects.equals(pairs.getRight(),null)&&pairs.getRight().contains(num)){//next group found
          HashMap<Address,Boolean> record=new HashMap<>();
          for(Address add:pairs.getLeft()){
            send(new TransferConfig(shardConfig,amoApplication_records.get(num),num),add);
            //send(new TransferConfig(shardConfig,sendamoApplication),add);
            record.put(add,false);
          }
          Ack_record.put(num,record);
          //Logger.getLogger("").info(this.address()+" sendamoapplication: "+ amoApplication_records.get(num)+" of this shard: "+num);
          break;//next shard
        }
      }
    }
    //set(new TransferConfigTimer(shardConfig,sendamoApplication),TransferConfigTimer.RERTY_MILLIS);
    //Logger.getLogger("").info(this.address()+" transfer amoapplication_record: "+ amoApplication_records);
  }
  private int findTheShard(Command comm){
    SingleKeyCommand singleKeyCommand = (SingleKeyCommand) comm;
    String key = singleKeyCommand.key();
    int theShard=keyToShard(key);
    return theShard;
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
