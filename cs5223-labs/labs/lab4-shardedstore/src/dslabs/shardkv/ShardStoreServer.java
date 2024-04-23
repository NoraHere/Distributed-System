package dslabs.shardkv;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Command;
import dslabs.framework.Result;
import dslabs.kvstore.KVStore;
import dslabs.kvstore.KVStore.KVStoreResult;
import dslabs.kvstore.KVStore.SingleKeyCommand;
import dslabs.kvstore.TransactionalKVStore;
import dslabs.kvstore.TransactionalKVStore.MultiGet;
import dslabs.kvstore.TransactionalKVStore.MultiGetResult;
import dslabs.kvstore.TransactionalKVStore.MultiPut;
import dslabs.kvstore.TransactionalKVStore.MultiPutOk;
import dslabs.kvstore.TransactionalKVStore.Swap;
import dslabs.kvstore.TransactionalKVStore.SwapOk;
import dslabs.kvstore.TransactionalKVStore.Transaction;
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
import org.checkerframework.checker.units.qual.C;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ShardStoreServer extends ShardStoreNode {
  private final Address[] group;
  private final int groupId;

  // Your code here...
  private static final String PAXOS_ADDRESS_ID = "paxos";
  private HashMap <String,Boolean> locks=new HashMap<>();//lock//part3
  private HashMap<AMOCommand,Boolean> holdLocks=new HashMap<>();//record if hold the lock
  private HashMap<AMOCommand,Boolean> votes=new HashMap<>();//votes //part3
  private HashMap<AMOCommand,Integer> tryTimes=new HashMap<>();//record tryTimes
  private HashMap<AMOCommand,HashMap<Integer,Boolean>> vote_results=new HashMap<>();//vote_result:AMOCommand:(tryTimes:result) //part3
  private HashMap<AMOCommand,HashMap<Integer,Boolean>> participants_vote_record=new HashMap<>();//AMOCommand:(GroupId:vote),need Paxos
  private HashMap<AMOCommand,Map<String, String>> readResult_record=new HashMap<>();//groupId:read data
  private HashMap<AMOCommand,AMOResult> shardStoreReply_records=new HashMap<>();
  private HashMap<Address,AMOCommand> requests=new HashMap<>();//record multikey requests
  //private HashMap<AMOCommand,HashMap<Integer,HashMap<Address,Boolean>>> part_vote_records=new HashMap<>();//coordinator record participants' votes//part3
  private Address paxosAddress;
  private KVStore kvStore=new KVStore();
  private TransactionalKVStore transactionalKVStore=new TransactionalKVStore();
  //private AMOApplication<?> amoApplication=new AMOApplication<>(kvStore);
  //private AMOApplication<?> sendamoApplication;
  private HashMap<Integer,AMOApplication<?>>amoApplication_records=new HashMap<>();//shards:amo
  private AMOResult res;
  private Address[] shardMasters;
  private int nextConfigNum= ShardMaster.INITIAL_CONFIG_NUM;//ask current config number(query)
  private int ackReConfigNum=-1;//configNum received all ack(successful sent)
  private ShardConfig currentShardConfig;
  private Set<Integer> shards;//corresponding shards
  private HashMap<Integer,HashMap<Address,Boolean>> Ack_record=new HashMap<>();//shard:<Add:true/false>
  private int seqNum=0;//reConfig seqNum
  private boolean firstTime=true;
  private boolean allReceived=false;//successful received all shards
  LinkedList<Command> commandList = new LinkedList<>();
  private boolean duringReconfiguration=false;
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
    if(!Objects.equals(currentShardConfig,null)){//already received first reply from shardMaster
      AMOCommand comm = (AMOCommand) m.command();
      if(Objects.equals(m.shardNum(),currentShardConfig.configNum())){//with same page
        if(comm.command() instanceof SingleKeyCommand){
          SingleKeyCommand singleKeyCommand = (SingleKeyCommand) comm.command();
          String key = singleKeyCommand.key();
          int theShard = keyToShard(key);
          Set<Integer> newShards;//in case during reconfiguration process
          if(!Objects.equals(currentShardConfig.groupInfo().get(this.groupId),null)){
            newShards=currentShardConfig.groupInfo().get(this.groupId).getRight();
          }
          else{
            newShards=null;
          }
          if(Objects.equals(newShards,null)||!newShards.contains(theShard)){
            res=new AMOResult(null,AMOCommand.getSequenceNum(comm),AMOCommand.getAddress(comm));
            send(new ShardStoreReply(res,false,currentShardConfig.configNum()),sender);
          }
          else{
            handleMessage(new PaxosRequest(m.command()), paxosAddress);
          }
        }
        else {//part3
          if(shardStoreReply_records.containsKey(comm)){
            send(new ShardStoreReply(shardStoreReply_records.get(comm),true,currentShardConfig.configNum()),sender);
          }
          else if(!requests.containsKey(sender)||requests.get(sender).sequenceNum()<comm.sequenceNum()){
            handleMessage(new PaxosRequest(m.command()),paxosAddress);
            requests.put(sender,comm);
          }
        }
      }
      else if(m.shardNum()>currentShardConfig.configNum()){//client's configNum is larger
        //checkIn();
        return;
      }
      else{//the client's configNum is out of date
        res = new AMOResult(null, AMOCommand.getSequenceNum(comm), AMOCommand.getAddress(comm));
        send(new ShardStoreReply(res, false,currentShardConfig.configNum()), sender);
      }
    }
    else{
      //checkIn();
      return;
    }
  }

  // Your code here...
  private void handlePaxosReply(PaxosReply m,Address sender){
    //from shardMaster
    if(m.result()instanceof ShardMaster.Error){
      return;
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
        currentShardConfig=shardConfig;
        nextConfigNum++; duringReconfiguration=false;
        if(!Objects.equals(shards,null)){//initialize amoApplication_records
          for(Integer shard:shards){
            amoApplication_records.put(shard,new AMOApplication<>(transactionalKVStore));//part3
          }
        }
      }
      else{
        if(nextConfigNum-1<shardConfig.configNum()&&firstTime){//shardMaster has changes
          firstTime=false;
          currentShardConfig=shardConfig;
          seqNum++;
          reconfig reconfig=new reconfig(shardConfig);//command
          AMOCommand comm=new AMOCommand(reconfig,seqNum,this.address());
          handleMessage(new PaxosRequest(comm),paxosAddress);
        }
      }
    }
  }
  private void handlePaxosRequest(PaxosRequest m,Address sender){
    //this is reply from local paxos: next operation to execute
    if (AMOCommand.getCommand((AMOCommand) m.command()) instanceof ShardStoreServer.ackReconfig) {//all ack
      ackReconfig ack=((ackReconfig) AMOCommand.getCommand((AMOCommand) m.command()));
      int mayAckReConfigNum=ack.shardConfig.configNum();//record current ackReConfigNum
      if(mayAckReConfigNum>ackReConfigNum&&Objects.equals(groupId,ack.groupId)){
        ackReConfigNum=mayAckReConfigNum;
        Logger.getLogger("").info(this.address()+" new ackReConfigNum: "+ackReConfigNum);
        Logger.getLogger("").info(this.address()+" all successful ack: " + ackReConfigNum);

        Set<Integer> newshards;
        if(!Objects.equals(ack.shardConfig.groupInfo().get(this.groupId),null)){
          newshards=ack.shardConfig.groupInfo().get(this.groupId).getRight();
        }
        else{
          newshards=null;
        }
        if(allReceived||Objects.equals(newshards,null)){//all received/no need to receive, next query
          allReceived=true;
        }
        checkAllDone();
      }
    }
    else{
      commandList.add(m.command());
      executeCommandList();
    }

  }

  private void handlePrepareMessage(PrepareMessage m,Address sender){//part3
    //participant receive PREPARE message
    Transaction comm=(Transaction)m.comm().command();
    Set<Integer> theShard=new HashSet<>();
    for(String keys:comm.keySet()){
      theShard.add(keyToShard(keys));
    }
    if(!Objects.equals(currentShardConfig,null)&&Objects.equals(currentShardConfig.configNum(),m.shardConfigNum())){
      //if(currentShardConfig.groupInfo().get(this.groupId).getRight().contains(theShard)){//verify it is the coordinator
      HashMap<Integer,Boolean> record;//record votes
      if(!vote_results.containsKey(m.comm())){
        record=new HashMap<>();
      }
      else{
        record=vote_results.get(m.comm());
      }
      //get the locks
      if(canAcquireLocks(m.comm())){//can access the lock
        isReleaseLocks(m.comm(),false);//acquire locks
        votes.put(m.comm(),true);//send prepareReply message to paxos?no need
        send(new PrepareReply(true,m.comm(),this.groupId,m.tryTimes(),currentShardConfig.configNum(),executeRead(m.comm())),sender);
        if(!record.containsKey(m.tryTimes())){//wait for decision
          record.put(m.tryTimes(),null);
          vote_results.put(m.comm(),record);
          set(new DecisionTimer(m.comm(),m.tryTimes(),sender,currentShardConfig.configNum()),DecisionTimer.RERTY_MILLIS);//timer to receive DecisionMessage
        }
      }
      else{//can not access the lock
        votes.put(m.comm(),false);//send prepareReply message to paxos?no need
        send(new PrepareReply(false,m.comm(),this.groupId,m.tryTimes(),currentShardConfig.configNum(),new HashMap<>()),sender);//vote no
        record.put(m.tryTimes(),false);
        vote_results.put(m.comm(),record);
      }
    }
    else{//configNum different
      //checkIn();???
      return;
      //send(new PrepareReply(false,m.comm(),this.groupId,m.tryTimes(),currentShardConfig.configNum(),new HashMap<>()),sender);
    }
  }
  private void handlePrepareReply(PrepareReply m,Address sender){//part3
    if(Objects.equals(currentShardConfig.configNum(),m.shardConfigNum())) {
      if(Objects.equals(m.tryTimes(),tryTimes)&&!participants_vote_record.get(m.comm()).containsKey(m.groupId())){
        //first time receive the reply of this tryTime
        participants_vote_record.get(m.comm()).put(m.groupId(),m.vote());//deal with vote
        if(!m.vote()){//should send decision
          sendDecisionMessage(new DecisionMessage(false,m.tryTimes(),m.comm(), currentShardConfig.configNum()));
        }
        else{//vote yes
          if(m.comm().command().readOnly()){//is MultiGet,deal with read data
            Map<String, String> mergedResult=new HashMap<>();
            if(readResult_record.containsKey(m.comm())){
              mergedResult= readResult_record.get(m.comm());
            }
            mergedResult.putAll(m.res());
            readResult_record.put(m.comm(),mergedResult);
          }
          if(checkPrepareReplyAllYes(m.comm())){
            sendDecisionMessage(new DecisionMessage(true,m.tryTimes(),m.comm(),currentShardConfig.configNum()));
            //sum up result(read data)
            Result res;
            if(m.comm().command().readOnly()){
              res=new MultiGetResult(readResult_record.get(m.comm()));
            }
            else if(m.comm().command() instanceof Swap){
              res=new SwapOk();
            }
            else{//MultiPut
              res=new MultiPutOk();
            }
            AMOResult result=new AMOResult(res,m.comm().sequenceNum(),m.comm().address());
            send(new ShardStoreReply(result,true,currentShardConfig.configNum()),m.comm().address());
            shardStoreReply_records.put(m.comm(),result);
            commandList.removeFirst();
            if(!commandList.isEmpty()){
              executeCommandList();
            }
          }
        }
      }
      else{
        return;//old message
      }
    }
    else if(currentShardConfig.configNum()<m.shardConfigNum()){
        //checkIn();
        return;
    }
//    else{//resend prepare???
//      HashMap<Integer,Set<Address>> servers=findParticipantServers(m.comm());
//      if(Objects.equals(servers,null)){//when currentConfig is null
//        checkIn();
//      }
//      else{
//        for(Integer groupId:servers.keySet()){
//          HashMap<Address,Boolean> record=new HashMap<>();
//          for(Address add:servers.get(groupId)){
//            record.put(add,null);
//            send(new PrepareMessage(tranc,tryTimes, currentShardConfig.configNum()),add);
//          }
//          part_vote_records.put(groupId,record);
//        }
//      }
//    }
  }
  private void handleDecisionMessage(DecisionMessage m,Address sender){//part3
    //tryTimes!!!
    if(Objects.equals(currentShardConfig.configNum(),m.shardConfigNum())){
      if(m.decision()){//can execute transaction
        Transaction tranc=(Transaction)m.comm().command();
        if(!tranc.readOnly()){
          executeWrite(m.comm());
        }
        isReleaseLocks(m.comm(),true);//release locks
        HashMap<Integer,Boolean> record=vote_results.get(m.comm());
        record.put(m.tryTimes(),true);
        vote_results.put(m.comm(),record);
      }
      else{//abort
        if(holdLocks.get(m.comm())){
          isReleaseLocks(m.comm(),true);//release the lock
        }
        HashMap<Integer,Boolean> record=vote_results.get(m.comm());
        record.put(m.tryTimes(),false);
        vote_results.put(m.comm(),record);
      }
    }
    else if(currentShardConfig.configNum()<m.shardConfigNum()){
      //checkIn();
      return;
    }
  }
  private void handleAskDecisionMessage(AskDecisionMessage message,Address sender){
    //coordinator
    if(vote_results.containsKey(message.comm())&&!Objects.equals(vote_results.get(message.comm()).get(message.tryTime()),null)){
      send(new DecisionMessage(vote_results.get(message.comm()).get(message.tryTime())
          ,message.tryTime(),message.comm(),currentShardConfig.configNum()),sender);
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
      ackReconfig ack=new ackReconfig(m.shardConfig(),groupId);//command
      seqNum++;
      handleMessage(new PaxosRequest(new AMOCommand(ack,seqNum,this.address())),paxosAddress);//consensus successful transfer
      Logger.getLogger("").info(this.address()+ "all ack of shardNum: "+ack.shardConfig.configNum());
    }
  }
  private void handleTransferConfig(TransferConfig m,Address sender){
    //new group servers receive
    if(nextConfigNum>m.shardConfig().configNum()||amoApplication_records.containsKey(m.theShard())){//old message
      send(new ACKReconfig(true,m.shardConfig(),m.theShard()),sender);
      Logger.getLogger("").info("old Transfer message, nextReConfigNum: "+nextConfigNum);
      return;
    }
    else if(m.shardConfig().configNum()>nextConfigNum){//first need to get the latest config from shardMaster
      //checkIn();//ensure my shards have already sent
      //Logger.getLogger("").info("checkIn()");
      return;
    }
    else{//nextConfigNum-1==m.shardConfig.configNum
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
    boolean checkReceived=true;
    for(Integer num:m.shardConfig().groupInfo().get(this.groupId).getRight()){
      if(!amoApplication_records.keySet().contains(num)){
        checkReceived=false;
        break;
      }
    }
    if(checkReceived){//all received
      allReceived=true;
      Logger.getLogger("").info(this.address()+" all received, configNum: "+m.shardConfig().configNum());
    }
    checkAllDone();
  }
  /* ---------------------------------------------------------------------------q--------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  // Your code here...
  private void onCheckInTimer(CheckInTimer t){
    //periodically send query to shardMaster
    if(!duringReconfiguration){
      checkIn();
    }
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
    set(t,TransferConfigTimer.RERTY_MILLIS);
  }
  private void onPrepareMessageTimer(PrepareMessageTimer t){//part3
    if(Objects.equals(currentShardConfig,null)||!Objects.equals(t.configNum(),currentShardConfig.configNum())){
      return;
    }
    if(checkPrepareReplyAllYes(t.comm())){
      return;
    }
    else{//retry
      tryTimes.put(t.comm(),t.tryTimes()+1);
      participants_vote_record.put(t.comm(),new HashMap<>());
      sendDecisionMessage(new DecisionMessage(false,t.tryTimes(),t.comm(),t.configNum()));//send DecisionMessage
      sendPrepareMessage(t.comm(),t.tryTimes()+1,t.configNum());
    }
  }
  private void onDecisionTimer(DecisionTimer t){//part3
    //wait for decision!!!
    int maxTryTime=-1;
    for(Integer num:vote_results.get(t.comm()).keySet()){
      if(num>maxTryTime){
        maxTryTime=num;
      }
    }
    if (Objects.equals(vote_results.get(t.comm()).get(maxTryTime),null)){//only ask the maxTryTime decision
      send(new AskDecisionMessage(t.comm(),maxTryTime,currentShardConfig.configNum()),t.coordinatorAdd());
      set(t,DecisionTimer.RERTY_MILLIS);
    }
  }
  /* -----------------------------------------------------------------------------------------------
   *  Utils
   * ---------------------------------------------------------------------------------------------*/
  // Your code here...

  private void checkAllDone(){
    Logger.getLogger("").info(this.address()+" checkAllDone");
    if(allReceived&&Objects.equals(ackReConfigNum,nextConfigNum)){//Received and sent, can move to next query
      Ack_record.clear();
      if(!Objects.equals(currentShardConfig.groupInfo().get(this.groupId),null)){//update shards
        shards=currentShardConfig.groupInfo().get(this.groupId).getRight();
      }
      else{
        shards=null;
      }

      if(!Objects.equals(shards,null)){
        Iterator<Integer> iterator = amoApplication_records.keySet().iterator();//update amoApplication_records
        while (iterator.hasNext()) {
          Integer shard = iterator.next();
          if (!shards.contains(shard)) {
            iterator.remove(); // Removes the current key safely
          }
        }
        //Logger.getLogger("").info("amoApplication_records after clearance: "+amoApplication_records);
      }
      else{
        amoApplication_records.clear();
      }
      nextConfigNum++;firstTime=true;allReceived=false;
      duringReconfiguration=false;//stop reconfiguration process
      Logger.getLogger("").info(this.address()+ " move to nextConfigNum: "+ nextConfigNum);
      //Logger.getLogger("").info("amoApplictaion_records: "+amoApplication_records);
      commandList.removeFirst();
      if(!commandList.isEmpty()){
        executeCommandList();
      }
    }
  }
  private void checkIn(){
    Query query=new Query(nextConfigNum);//ask shardMaster next shards configuration
    for(Address add:this.shardMasters){
      send(new PaxosRequest(query),add);
    }
  }
  private void reconfiguration(ShardConfig shardConfig){
    Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo=shardConfig.groupInfo();
    Logger.getLogger("").info(this.address()+"reconfiguration");
    Logger.getLogger("").info(this.address()+" shards: "+shards);
    Logger.getLogger("").info(this.address()+"pair: "+groupInfo.get(this.groupId));
    // groupId -> <group members, shard numbers>
    if(Objects.equals(shards, null)){//no need to transfer
      ackReConfigNum=shardConfig.configNum();
      Logger.getLogger("").info(this.address()+" new ackReConfigNum: "+ackReConfigNum);
      Set<Integer> newshards;
      if(!Objects.equals(shardConfig.groupInfo().get(this.groupId),null)){
        newshards=shardConfig.groupInfo().get(this.groupId).getRight();
      }
      else{
        newshards=null;
      }
      if(Objects.equals(newshards,null)){//no need to receive,can move to next query
        allReceived=true;
        Logger.getLogger("").info(this.address()+" all received");
      }
    }
    else{//shards!=null
      Pair<Set<Address>, Set<Integer>> pair= groupInfo.get(this.groupId);
      if(Objects.equals(pair,null)||Objects.equals(pair.getRight(),null)){//not involve anymore,leave or never show up
        allReceived=true;//no need to received
        Logger.getLogger("").info(this.address()+" all received");
        transferConfig(shardConfig);
        set(new TransferConfigTimer(shardConfig),TransferConfigTimer.RERTY_MILLIS);
      }
      else{//pair.getRight!=null
        Set<Integer> nextShards=pair.getRight();//nextShards!=null
          boolean noNeedR=shards.containsAll(nextShards);
          boolean noNeedS=nextShards.containsAll(shards);
        Logger.getLogger("").info(this.address()+" noNeedR: "+noNeedR+" noNeedS: "+noNeedS);
          if(noNeedR){
            allReceived=true;
            Logger.getLogger("").info(this.address()+" all received");
          }
          if(noNeedS){
            ackReConfigNum=shardConfig.configNum();
            Logger.getLogger("").info(this.address()+" new ackReConfigNum: "+ackReConfigNum);
          }
          else{
            transferConfig(shardConfig);
            set(new TransferConfigTimer(shardConfig),TransferConfigTimer.RERTY_MILLIS);
          }
        }
    }
    checkAllDone();
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
  private void executeCommandList(){
    if(commandList.isEmpty()){
      return;
    }
    AMOCommand comm=(AMOCommand) commandList.getFirst();//firstCommand
    Logger.getLogger("").info(this.address()+" execute command: "+comm);
    if(AMOCommand.getCommand(comm) instanceof ShardStoreServer.reconfig){
      Logger.getLogger("").info(this.address()+" duringReconfiguration: "+duringReconfiguration);
      if(!duringReconfiguration){
        duringReconfiguration=true;
        reconfig newreconfig= (ShardStoreServer.reconfig) AMOCommand.getCommand(comm);
        reconfiguration(newreconfig.shardConfig);
      }
      checkAllDone();
    }
    else{
      if(comm.command() instanceof Transaction){//part3
        participants_vote_record.put(comm,new HashMap<>());
        tryTimes.put(comm,1);
        if(!Objects.equals(currentShardConfig,null)){
          sendPrepareMessage(comm,1,currentShardConfig.configNum());
        }
        else{
          //checkIn();
          return;
        }
      }
      else if(comm.command() instanceof SingleKeyCommand){
        int theShard=findTheShard(comm.command());
        if(!Objects.equals(amoApplication_records.get(theShard),null)){
          res=amoApplication_records.get(theShard).execute(comm);
          //res=amoApplication.execute(m.command());
          send(new ShardStoreReply(res,true,currentShardConfig.configNum()),AMOResult.getAddress(res));//send back to client
          commandList.removeFirst();
          if(!commandList.isEmpty()){
            executeCommandList();
          }
        }
      }
    }
  }
  private int findTheShard(Command comm){
    SingleKeyCommand singleKeyCommand = (SingleKeyCommand) comm;
    String key = singleKeyCommand.key();
    int theShard=keyToShard(key);
    return theShard;
  }
  private HashMap<Integer,Set<Address>> findParticipantServers(Command command){//part3
    if(Objects.equals(currentShardConfig,null)){// groupId -> <group members, shard numbers>
      return null;
    }
    Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo=currentShardConfig.groupInfo();
    if(command instanceof Transaction) {//transactional requests, part3
      // groupId -> <group members, shard numbers>
      HashMap<Integer,Set<Address>> servers = new HashMap<>();//groupId:servers
      for (String key : ((Transaction) command).keySet()) {
        int theShard = keyToShard(key);
        for(Integer groupId: groupInfo.keySet()){
          if(!Objects.equals(groupInfo.get(groupId).getRight(),null)&&groupInfo.get(groupId).getRight().contains(theShard)){
            servers.put(groupId,groupInfo.get(groupId).getLeft());
          }
        }
      }
      return servers;
    }
    return null;//only deal with transaction
  }
  private boolean checkPrepareReplyAllYes(AMOCommand comm){//part3
    HashMap<Integer,Set<Address>> servers=findParticipantServers(comm.command());
    if(Objects.equals(servers,null)){//ERROR
      Logger.getLogger("").info("ERROR Find Participant Servers");
    }
    boolean allYes=true;
    for(Integer groupId:participants_vote_record.get(comm).keySet()){
      if(participants_vote_record.get(comm).containsValue(null)||participants_vote_record.get(comm).containsValue(false)){
        allYes=false;
        break;
      }
    }
    return allYes;
  }
  private boolean canAcquireLocks(AMOCommand comm){//part3
    if(Objects.equals(currentShardConfig.groupInfo().get(this.groupId),null)){
      return false;
    }
    Boolean allKeysOK=true;
    for(String key:((Transaction) comm.command()).keySet()){
      if(locks.containsKey(key)&&!locks.get(key)){
        allKeysOK=false;
        break;
      }
    }
    return allKeysOK;
  }
  private void isReleaseLocks(AMOCommand comm,Boolean release){//part3
    //Logger.getLogger("").info(this.address()+" isReleaseLocks: "+release);
    if(Objects.equals(currentShardConfig.groupInfo().get(this.groupId),null)){
      return;
    }
    for (String key : ((Transaction) comm.command()).keySet()) {//acquire locks
      int theShard = keyToShard(key);
      if(currentShardConfig.groupInfo().get(this.groupId).getRight().contains(theShard)){//is the key responsible
        locks.put(key,release);
        //Logger.getLogger("").info(this.address()+" change the key: "+key+" to "+release);
      }
    }
    holdLocks.put(comm,!release);
  }
  private Map<String, String> executeRead(AMOCommand comm){//part3
    Transaction tranc=(Transaction) comm.command();
    Map<String, String> mergedResult = new HashMap<>();//sum up
    if(!tranc.readOnly()){//no read
      return null;
    }
    if(comm.command() instanceof MultiGet) {
      Set<Integer> theShard=new HashSet<>();
      for(String key:((Transaction) comm.command()).readSet()) {//execute and send back to client
        theShard.add(keyToShard(key));
      }
      for(Integer shard:theShard){
        if(amoApplication_records.containsKey(shard)){//shard responsible
          MultiGetResult results=(MultiGetResult)amoApplication_records.get(shard).execute(comm).result();
          mergedResult.putAll(results.values());
        }
      }
    }
//    else if(comm.command() instanceof Swap){//write
//      result=null;
//    }
//    else{
//      SingleKeyCommand singleKeyCommand = (SingleKeyCommand) comm.command();
//      String key = singleKeyCommand.key();
//      int theShard=keyToShard(key);
//      if(amoApplication_records.containsKey(theShard)){
//        result=amoApplication_records.get(theShard).execute(comm.command()).result();
//      }
//    }
    return mergedResult;
  }
  private void executeWrite(AMOCommand comm){//part3
    if(comm.command().readOnly()){
      return;
    }
    if(comm.command() instanceof Swap){
      for(String key:((Transaction) comm.command()).writeSet()){
        int shard=keyToShard(key);
        if(currentShardConfig.groupInfo().get(groupId).getRight().contains(shard)){//responsible for
          if(amoApplication_records.containsKey(shard)) {
            amoApplication_records.get(shard).execute(comm);
            Logger.getLogger("").info(this.address() + " execute " + comm + " shard: " + shard);
          }
          break;
        }
      }
    }
    else{//MultiPut
      for(Integer shards:currentShardConfig.groupInfo().get(this.groupId).getRight()){//only execute
        for(String key:((Transaction) comm.command()).writeSet()) {
          int shard=keyToShard(key);
          if(Objects.equals(shards,shard)) {//is the key responsible
            if(amoApplication_records.containsKey(shards)){
              amoApplication_records.get(shards).execute(comm);
              Logger.getLogger("").info(this.address()+" execute "+comm+" shard: "+shards);
            }
            break;
          }
        }
      }
    }

  }
  private void sendPrepareMessage(AMOCommand comm,int tryTimes,int configNum){
    //As coordinator
    // groupId -> <group members, shard numbers>
    HashMap<Integer,Set<Address>> servers=findParticipantServers(comm.command());
    if(Objects.equals(servers,null)){//when currentConfig is null
      //checkIn();
      return;
    }
    else if(Objects.equals(servers.size(),1)){//only one group involves
      HashMap<Integer,Boolean> record=new HashMap<>();
      //acquire locks and send reply to client
      Logger.getLogger("").info(this.address()+" canAcquireLocks: "+canAcquireLocks(comm));
      if(canAcquireLocks(comm)){
        isReleaseLocks(comm,false);//acquire locks
        //send commit to paxos???
        record.put(this.groupId,true);
        participants_vote_record.put(comm,record);
        Result res=new MultiGetResult(new HashMap<>());
        if(comm.command() instanceof MultiGet){
          res=new MultiGetResult(executeRead(comm));
        }
        else if(comm.command() instanceof Swap){
          res=new SwapOk();
        }
        else{//multiput
          res=new MultiPutOk();
        }
        executeWrite(comm);
        AMOResult result=new AMOResult(res,comm.sequenceNum(),comm.address());
        isReleaseLocks(comm,true);//release locks
        send(new ShardStoreReply(result, true,currentShardConfig.configNum()),comm.address());
        shardStoreReply_records.put(comm,result);
        commandList.removeFirst();
        if(!commandList.isEmpty()){
          executeCommandList();
        }
      }
      else{
        //wait for another try
        record.put(this.groupId,false);
        participants_vote_record.put(comm,record);
        set(new PrepareMessageTimer(comm,tryTimes,currentShardConfig.configNum()),PrepareMessageTimer.RERTY_MILLIS);
      }
    }
    else{//many groups involve, need to send PrepareMessages
      Transaction tranc=(Transaction) comm.command();
      HashMap<Integer,Boolean> record=new HashMap<>();//record for this AMOCommand
      for(Integer groupId:servers.keySet()){
        if(Objects.equals(groupId,this.groupId)){
          if(canAcquireLocks(comm)){
            isReleaseLocks(comm,false);//acquire locks
            record.put(this.groupId,true);
          }
          else{
            record.put(this.groupId,false);
            sendDecisionMessage(new DecisionMessage(false,tryTimes,comm,configNum));
          }
          //send to local Paxos?no need
        }
        else{
          record.put(groupId,null);
          for(Address add:servers.get(groupId)){
            send(new PrepareMessage(comm,tryTimes, configNum),add);
          }
        }
      }
      participants_vote_record.put(comm,record);
      set(new PrepareMessageTimer(comm,tryTimes,configNum),PrepareMessageTimer.RERTY_MILLIS);
    }
  }
  private void sendDecisionMessage(DecisionMessage message){//part3
    HashMap<Integer,Set<Address>> servers=findParticipantServers(message.comm());
    if(!Objects.equals(servers,null)){
      for(Integer groupId:servers.keySet()){
        if(Objects.equals(groupId,this.groupId)){
          if(message.decision()){
            executeWrite(message.comm());
            isReleaseLocks(message.comm(),true);//release lock
            HashMap<Integer,Boolean> record;
            if(vote_results.containsKey(message.comm())){
              record=vote_results.get(message.comm());
            }
            else{
              record=new HashMap<>();
            }
            record.put(message.tryTimes(),true);
            vote_results.put(message.comm(),record);//record result
          }
          else{//abort
            if(holdLocks.get(message.comm())){//release lock only when hold it
              isReleaseLocks(message.comm(),true);
            }
            HashMap<Integer,Boolean> record;
            if(vote_results.containsKey(message.comm())){
              record=vote_results.get(message.comm());
            }
            else{
              record=new HashMap<>();
            }
            record.put(message.tryTimes(),false);
            vote_results.put(message.comm(),record);//record result
          }
        }
        else{
          for(Address add:servers.get(groupId)){
            send(message,add);
          }
        }
      }
    }
    else{
      //checkIn();
      return;
    }
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
