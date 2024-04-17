package dslabs.paxos;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import dslabs.atmostonce.AMOApplication;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Node;
import dslabs.framework.Result;
import dslabs.shardmaster.ShardMaster.Query;
import dslabs.shardmaster.ShardMaster.ShardMasterCommand;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import java.util.logging.Logger;//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
import org.apache.commons.lang3.tuple.Pair;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)

public class PaxosServer extends Node {
  /** All servers in the Paxos group, including this one. */
  private final Address[] servers;

  // Your code here...
  private final Address address;//this address
  private Application app;
  private AMOApplication application;
  private boolean OneServer=false;
  private int cleared=0;//max cleared log num
  private HashMap<Integer,AMOCommand> decisions =new HashMap<>();//set of decisions
  private HashMap<Address,PaxosRequest> requests =new HashMap<>();//set of requests
  private int slot_in =1;//next propose
  private int slot_out =1;//next execute
  private AMOResult result;
  private double current_leader_ballot=0.0;//server think current leader's ballot number
  //////acceptors
  private double a_ballot=0;
  private HashMap<Integer,pvalue> accepted =new HashMap<>();//{slot_num:pvalues<b,s,c>,...}
  //////leaders
  private int leader_number;//this.number
  private int seq_num=0;//seqnum
  private boolean isActive = false;//is active leader
  private boolean election=false;//is in process of election
  private double l_ballot=0;//election number as leader
  private boolean dis_alive_f=false;
  private boolean dis_alive_s=false;
  private  boolean stop_phase1a_timer=false;
  private HashMap<Integer,AMOCommand> l_proposals =new HashMap<>();//set of proposals
  private HashMap<Address, Phase1b> phase1b_record=new HashMap<>();//record each server'phase1b reply
  private Multimap<Integer,Pair<Address, Phase2b>> phase2b_record= ArrayListMultimap.create();//record each server'phase2b reply
  private HashMap<Address,Integer> statistic=new HashMap<>();//count heartbeatReply and decide cleared
  private Address sendAdd;



  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public PaxosServer(Address address, Address[] servers, Application app) {
    super(address);
    this.servers = servers;

    // Your code here...
    this.address=address;
    this.app=app;
    application=new AMOApplication<>(this.app);
  }

  //for lab4
  public PaxosServer(Address address,Address[] servers, Address sendAdd){
    super(address);
    this.servers=servers;
    this.address=address;
    this.sendAdd=sendAdd;
  }

  @Override
  public void init() {
    // Your code here...
    if(servers.length==1){//one-server
      OneServer=true;
    }
    else{
      for (int i=0;i<servers.length;i++){//generate leader_num
        if (Objects.equals(servers[i],this.address)) {
          leader_number = i+1;//start from 1, server1: 0.1, ...
          break;
        }
      }
      double num=seq_num+0.1*leader_number;
      l_ballot=num;
      elect(num);
      stop_phase1a_timer=false;
      election=true;
    }
  }

  /* -----------------------------------------------------------------------------------------------
   *  Interface Methods
   *
   *  Be sure to implement the following methods correctly. The test code uses them to check
   *  correctness more efficiently.
   * ---------------------------------------------------------------------------------------------*/

  /**
   * Return the status of a given slot in the server's local log.
   *
   * <p>If this server has garbage-collected this slot, it should return {@link
   * PaxosLogSlotStatus#CLEARED} even if it has previously accepted or chosen command for this slot.
   * If this server has both accepted and chosen a command for this slot, it should return {@link
   * PaxosLogSlotStatus#CHOSEN}.
   *
   * <p>Log slots are numbered starting with 1.
   *
   * @param logSlotNum the index of the log slot
   * @return the slot's status
   * @see PaxosLogSlotStatus
   */
  public PaxosLogSlotStatus status(int logSlotNum) {
    // Your code here...
    if(logSlotNum<=cleared) {
      return PaxosLogSlotStatus.CLEARED;
    }
    else if(decisions.containsKey(logSlotNum)){
      return PaxosLogSlotStatus.CHOSEN;
    }
    else if(accepted.containsKey(logSlotNum)){
      return PaxosLogSlotStatus.ACCEPTED;
    }
    else {
      return PaxosLogSlotStatus.EMPTY;
    }
  }

  /**
   * Return the command associated with a given slot in the server's local log.
   *
   * <p>If the slot has status {@link PaxosLogSlotStatus#CLEARED} or {@link
   * PaxosLogSlotStatus#EMPTY}, this method should return {@code null}. Otherwise, return the
   * command this server has chosen or accepted, according to {@link PaxosServer#status}.
   *
   * <p>If clients wrapped commands in {@link dslabs.atmostonce.AMOCommand}, this method should
   * unwrap them before returning.
   *
   * <p>Log slots are numbered starting with 1.
   *
   * @param logSlotNum the index of the log slot
   * @return the slot's contents or {@code null}
   * @see PaxosLogSlotStatus
   */
  public Command command(int logSlotNum) {
    // Your code here...
    if(Objects.equals(status(logSlotNum),PaxosLogSlotStatus.EMPTY)
        ||Objects.equals(status(logSlotNum),PaxosLogSlotStatus.CLEARED)){
      return null;
    }
    else{
      if(decisions.containsKey(logSlotNum)){
        return AMOCommand.getCommand(decisions.get(logSlotNum));
      }
      else{
        return AMOCommand.getCommand(accepted.get(logSlotNum).com);
      }
    }
  }

  /**
   * Return the index of the first non-cleared slot in the server's local log. The first non-cleared
   * slot is the first slot which has not yet been garbage-collected. By default, the first
   * non-cleared slot is 1.
   *
   * <p>Log slots are numbered starting with 1.
   *
   * @return the index in the log
   * @see PaxosLogSlotStatus
   */
  public int firstNonCleared() {
    // Your code here...
    //largest num of cleared/ accepted.keys/ decisions.keys
    return cleared+1;
  }

  /**
   * Return the index of the last non-empty slot in the server's local log, according to the defined
   * states in {@link PaxosLogSlotStatus}. If there are no non-empty slots in the log, this method
   * should return 0.
   *
   * <p>Log slots are numbered starting with 1.
   *
   * @return the index in the log
   * @see PaxosLogSlotStatus
   */
  public int lastNonEmpty() {
    // Your code here...
    int num=Math.max(cleared,findMaxKey(decisions));
    num=Math.max(num,findMaxKey(accepted));
    return num;
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void handlePaxosRequest(PaxosRequest m, Address sender) {
    // Your code here...
    //As Leaders
    if(m.command instanceof AMOCommand){
      AMOCommand comm =(AMOCommand) m.command;
      if(!Objects.equals(application,null)&&application.alreadyExecuted(comm)){
        send(new PaxosReply(application.execute(comm)),sender);
        return;
      }
      if (OneServer) {
        isActive = true;
        current_leader_ballot=l_ballot;
        requests.put(sender,m);
        accepted.put(slot_in,new pvalue(l_ballot,slot_in,comm));
        perform(new Decision(slot_in,comm));
        slot_in++;
      }
      else {
        //all leaders save requests{address:request}
        if ((!requests.containsKey(sender) || requests.get(sender) != m)
            &&!decisions.containsValue(comm)) {
          for (pvalue pv : accepted.values()) {//command not in accepted/decisions/requests
            if (pv.com.equals(comm)) {
              return;
            }
          }
          requests.put(sender, m);
        }

        //distinguished leader propose,and save in l_proposals{slot:command,...}
        if (isActive) {
          if (!l_proposals.containsValue(comm)&&!decisions.containsValue(comm)) {//never proposed
            findSlot_in();
            l_proposals.put(slot_in, comm);
            propose(l_ballot,slot_in,comm);
          }
        }
      }
    }
    if(m.command instanceof Query){//query
      Result res=app.execute(m.command);
      Logger.getLogger("").info("query_config: " + res);
      send(new PaxosReply(res),sender);//send back directly
    }
  }

  // Your code here...

  private void handlePhase1a(Phase1a message,Address sender){
    //AS Acceptors
    if(a_ballot<message.ballot_num()){
      a_ballot=message.ballot_num();
      for(Integer num:message.accepted().keySet()){//???
        if(!accepted.containsKey(num)){
          accepted.put(num,message.accepted().get(num));
        }
      }
    }
    send(new Phase1b(a_ballot,accepted,decisions),sender);
    //Logger.getLogger("SendP1b in Phase1a").info("SendP1b(b,accepted): " + a_ballot+", "+accepted);
  }
  private void handlePhase1b(Phase1b message,Address sender){
    //AS Leaders
    if(!election)return;//not elect anymore
    for (Integer num:message.decisions().keySet()){//update decisions
      if(!decisions.containsKey(num)){
        perform(new Decision(num,message.decisions().get(num)));
      }
    }
    if(phase1b_record.containsKey(sender)//old messages
        &&(message.ballot_num()<=phase1b_record.get(sender).ballot_num())) {return;}
    if(message.ballot_num()<l_ballot)return;//???
    phase1b_record.put(sender,message);
    //Logger.getLogger("").info("phase1b_record: " +this.address+" , "+ phase1b_record);
    count_1(phase1b_record);
}

  private void handlePhase2a(Phase2a message,Address sender){
    //AS Acceptors
    //sender think it is active leader
    if (Objects.equals(message.ballot_num(),a_ballot)){
      current_leader_ballot=message.ballot_num();//???
      pvalue pv=new pvalue(message.ballot_num(),message.slot_num(),message.com());
      accepted.put(message.slot_num(),pv);
    }
    send(new Phase2b(a_ballot,message.slot_num()),sender);
    //Logger.getLogger("Message").info(this.address+" Send P2b Message(b,s): " + a_ballot+", "+message.slot_num());
  }

  private void handlePhase2b(Phase2b message,Address sender){
//    if(message.current_leader_b()>l_ballot){//???
//      isActive = false;
//      set(new CheckActive(), CheckActive.RETRY_MILLIS);
//    }
    if (!isActive){return;}//As Distinguished Leaders
    if(message.ballot_num()<l_ballot){//old messages
      return;
    }
    // Logger.getLogger("Receive P2b").info("P2b(s,b): " + message.slot_num()+", "+message.ballot_num());
    Integer slot_num=message.slot_num();
    Pair<Address, Phase2b> pair = Pair.of(sender, message);
    if (!phase2b_record.containsKey(slot_num)){
      phase2b_record.put(slot_num, pair);
    } else{//contain slot_num
      boolean found=false;//found the sender
      Pair<Address, Phase2b> toRemove=null;//???
      for(Pair<Address, Phase2b> pairs:phase2b_record.get(slot_num)){
        if(Objects.equals(sender,pairs.getLeft())){//has sender
          found=true;
          if(pairs.getRight().ballot_num()<message.ballot_num()){
            toRemove=pairs;
//            copyList.remove(slot_num,pairs);//remove old pair
//            copyList.put(slot_num, pair);//put new pair
          }
          break;
        }
      }
      if(!Objects.equals(toRemove,null)){
        phase2b_record.remove(slot_num, toRemove);
        phase2b_record.put(slot_num,pair);
      }

      if(!found){
        phase2b_record.put(slot_num, pair);
      }
    }
    count_2(phase2b_record);
  }
  private void handleDecision(Decision message,Address sender){
    if(!decisions.containsKey(message.slot())&&message.slot()>cleared){//havn't performed
      perform(message);
    }
  }
  private void handleHeartbeat(Heartbeat message,Address sender){
    if(message.ballot()>=current_leader_ballot&&message.ballot()>=a_ballot){//???
      dis_alive_f = true;
      current_leader_ballot=message.ballot();
    }
//    if(message.ballot()>=a_ballot) {
//      dis_alive_f = true;
//      current_leader_ballot=message.ballot();//???
//    }
    if(message.ballot()>l_ballot){
      if(election){//stop election
        election=false;
        stop_phase1a_timer=true;
        dis_alive_f=true;
        set(new CheckActive(), CheckActive.RETRY_MILLIS);
      }
      if(isActive) {
        isActive = false;
        a_ballot=message.ballot();
        dis_alive_f=true;
        statistic.clear();
        set(new CheckActive(), CheckActive.RETRY_MILLIS);
      }
    }
    if(cleared<message.cleared()){
      cleared=message.cleared();
      garbage_collect(cleared);
    }

    if(Objects.equals(current_leader_ballot,0.0)||election){//??????????
      send(new HeartbeatReply(slot_out,a_ballot),sender);
    }
    else{
      send(new HeartbeatReply(slot_out,current_leader_ballot),sender);
    }
    //send(new HeartbeatReply(slot_out,current_leader_ballot),sender);//???
  }
  private void handleHeartbeatReply(HeartbeatReply message,Address sender){
    //distinguished leader
    if(message.ballot()>l_ballot){//???
      isActive = false;
      //election=false;
      dis_alive_f=false;
      set(new CheckActive(), CheckActive.RETRY_MILLIS);
    }
    if(!isActive)return;
    //Sum up smallest slot_out among all reply
    statistic.put(this.address,slot_out);//add this.address to statistic
    statistic.put(sender,message.slot_out());//add this message
    if(Objects.equals(statistic.size(),servers.length)){//received all heartbeatReply
      int min = Integer.MAX_VALUE;//smallest slot_out
      for (Integer value : statistic.values()) {
        if (value < min) {
          min = value; // Update min if current element is smaller
        }
      }
      if(min>cleared+1) {//cleared increase
        cleared = min - 1;
        garbage_collect(cleared);
      }
      statistic.clear();
    }
    int num=message.slot_out();
    while(num<slot_out){//the server is out of date, need to resend decision to it
      send(new Decision(num,decisions.get(num)),sender);
      num++;
    }

  }


  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  // Your code here...
private void onHeartBeatTimer(HeartBeatTimer t){
  if(!isActive){return;}
  for (Address add : servers) {//send HeartBeat to all leaders
    if (!add.equals(this.address)) {
      send(new Heartbeat(cleared,l_ballot), add);
    }
  }
  set(t,HeartBeatTimer.RETRY_MILLIS);
}

private void onCheckActive(CheckActive t){

  if(isActive){return;}//only follower leader need check active
  if(election||!stop_phase1a_timer){return;}//in process of election, no matter if is new l_ballot
  if(!dis_alive_f&&!dis_alive_s){//current distinguished leader dead
    seq_num++;
    double num=seq_num+0.1*leader_number;
    l_ballot=num;
    //Logger.getLogger("").info("Start new election(num): "+num +" by :"+this.address);
    //current_leader_ballot=l_ballot;//think self is potential current_leader???
    elect(num);
    stop_phase1a_timer=false;
    election=true;
    //phase1b_record.clear();???
    return;
  }
  set(t,CheckActive.RETRY_MILLIS);
  Logger.getLogger("").info("FinishCheckActiveTimer by: "+this.address+ " leader_alive: "+ (dis_alive_s||dis_alive_f)+" a_ballot: "+a_ballot);
  dis_alive_s=dis_alive_f;
  dis_alive_f=false;
}
private void onPhase1aTimer(Phase1aTimer t){
  //AS Leaders
  if(isActive||stop_phase1a_timer){return;}
  if(t.num()<l_ballot)return;
  //Logger.getLogger("").info("onPhase1aTimer of: " + this.address+" isActive: "+isActive+ " stop_p1a_timer "+stop_phase1a_timer);
  elect(t.num());
  stop_phase1a_timer=false;
}

private void onPhase2aTimer(Phase2aTimer t){
  //AS Leaders
  if(!isActive){return;}
  if(decisions.containsKey(t.slot_num())){return;}
  if(t.slot_num()<=cleared)return;//old message
  if(t.ballot_num()<l_ballot)return;//old propose
  propose(t.ballot_num(),t.slot_num(),t.com());

}
  /* -----------------------------------------------------------------------------------------------
   *  Utils
   * ---------------------------------------------------------------------------------------------*/
  // Your code here...

private void count_1(HashMap<Address, Phase1b> record) {
  //Statistic
  HashMap<String, Integer> counts = new HashMap<>();//count majority
  for (Address add : record.keySet()) {//count
    String key = Objects.equals((record.get(add)).ballot_num(), l_ballot) ? "D" : "P";
    counts.put(key, counts.getOrDefault(key, 0) + 1);
  }
  Logger.getLogger("").info("counts.D: "+this.address+ " : "+counts.getOrDefault("D",0));
  Logger.getLogger("").info("counts.P: "+this.address+ " : "+counts.getOrDefault("P",0));
  if (counts.containsKey("P") && counts.get("P") > servers.length / 2) {//received majority
    isActive = false;
    Logger.getLogger("").info("Stop Phase1: "+this.address+ " and checkActive");
    dis_alive_f=false;
    dis_alive_s=false;
    set(new CheckActive(),CheckActive.RETRY_MILLIS);
    stop_phase1a_timer = true;
    election=false;
    //phase1b_record.clear();
  }
  else if (counts.containsKey("D") && counts.get("D") > servers.length / 2) {
    Logger.getLogger("").info("Distinguished Leader: "+this.address +", ballot: "+ l_ballot);
    isActive = true;//???
    current_leader_ballot=l_ballot;
    stop_phase1a_timer = true;
    election=false;
    //phase1b_record.clear();
    //broadcast HeartBeat to all leaders
    for (Address add : servers) {
      if (!add.equals(this.address)) {
        send(new Heartbeat(cleared,l_ballot), add);
      }
    }
    set(new HeartBeatTimer(),HeartBeatTimer.RETRY_MILLIS);//resend broadcast
    //if p1b.accepted is not null, then propose the largest ballot_num command
    HashMap<Integer, pvalue> newaccepted= new HashMap<>();//{slot_num:pvalue,...}
    for (Phase1b p1b : phase1b_record.values()) {//build newaccepted
      if (!p1b.accepted().isEmpty()) {//accepted is not null
        // if (!Objects.equals(p1b.accepted(), null)) {//accepted is not null
        for (Integer slot_num : p1b.accepted().keySet()) {
          if(decisions.containsKey(slot_num)||slot_num<=cleared){
            continue;
          }
          pvalue pv = p1b.accepted().get(slot_num);
          if (!newaccepted.containsKey(slot_num)
              || newaccepted.get(slot_num).ballot_num < pv.ballot_num) {
            newaccepted.put(slot_num, pv);
          }
        }
      }
    }
    for(Integer slot: newaccepted.keySet()){//prioritize propose newaccepted
      AMOCommand comm=newaccepted.get(slot).com;
      l_proposals.put(slot,comm);
      propose(l_ballot,slot,comm);
    }
    //merge l_proposals with requests and propose
    for(PaxosRequest request:requests.values()){//combine with requests
      AMOCommand comm=(AMOCommand) request.command;
      if(!l_proposals.containsValue(comm)&&!decisions.containsValue(comm)){
        findSlot_in();
        int slot=slot_in;
        l_proposals.put(slot,comm);
        propose(l_ballot,slot,comm);
      }
    }

  }
}

  private void count_2(Multimap<Integer,Pair<Address,Phase2b>> phase2b_record) {
    //Statistic
    for (Integer slot_num : phase2b_record.keySet()) {
      if (phase2b_record.get(slot_num).size()
          > servers.length / 2) {//receive majority p2b of slot_num
        HashMap<String, Integer> counts = new HashMap<>();//count majority
        for (Pair<Address, Phase2b> pair : phase2b_record.get(slot_num)) {//build counts
          //Address address = pair.getLeft();
          Phase2b p2b = pair.getRight();
          String key = Objects.equals(p2b.ballot_num(), l_ballot) ? "D" : "P";
          counts.put(key, counts.getOrDefault(key, 0) + 1);
        }
        if (counts.containsKey("P") && counts.get("P") > servers.length / 2) {
          Logger.getLogger("").info( "Preempted"+this.address);
          isActive = false;
          dis_alive_f=false;
          dis_alive_s=false;
          set(new CheckActive(), CheckActive.RETRY_MILLIS);
          Logger.getLogger("").info("isActive may changed: "+this.address+ " "+isActive);
        } else if (counts.containsKey("D") && counts.get("D") > servers.length / 2) {//Distinguished
          isActive = true;
          if (l_proposals.containsKey(slot_num)&&!decisions.containsKey(slot_num)) {//send decision of slot_num
            AMOCommand comm = l_proposals.get(slot_num);
            Decision decision = new Decision(slot_num, comm);
            Logger.getLogger("").info("Decision: " + decision);
            for (Address add : servers) {
              if (!add.equals(this.address)) {
                send(decision, add);
              }
            }
            perform(decision);//self receive
          }
        }
      }
    }
  }

  private void elect(Double num){//leader start to elect distinguished leader
    //phase1b_record.clear();
    for (Address add : servers){
      if (! add.equals(this.address)){//send phase1a to all acceptors
        send(new Phase1a(num,accepted),add);
      }
    }
    set(new Phase1aTimer(num),Phase1aTimer.RETRY_MILLIS);//resend p1a until active_leader
    //pretend to handle phase1a as self.acceptor
    if (a_ballot < num) {//repeated
      a_ballot = num;
    }
    //pretend to receive phase1b as self.leader
    phase1b_record.put(this.address,new Phase1b(a_ballot,accepted,decisions));
    Logger.getLogger("").info("Finish ELECT by: "+this.address+ " , "+num );

    //update phase1b_record???
    List<Address> toRemove = new ArrayList<>();
    for(Address add:phase1b_record.keySet()){
      if(phase1b_record.get(add).ballot_num()<num){
        toRemove.add(add);
      }
    }
    for(Address add:toRemove){
      phase1b_record.remove(add);
    }
    //Logger.getLogger("").info("latest phase1b_record of: "+this.address+ " , "+phase1b_record );
  }
  private void propose(Double num, Integer slot, AMOCommand comm){//active leader start phase2
    for (Address add : servers) {//send phase2a to all acceptors
      if (!add.equals(this.address)) {
        send(new Phase2a(num,slot,comm), add);//<b,s,c>
      }
    }
    set(new Phase2aTimer(num,slot,comm),Phase2aTimer.RETRY_MILLIS);
    //pretend to handle phase2a as self.acceptor
    if(Objects.equals(a_ballot,num)){
      pvalue pv=new pvalue(num,slot,comm);
      accepted.put(slot,pv);
    }
    //pretend to receive phase2b as self.dleader
    Pair<Address, Phase2b> pair = Pair.of(this.address, new Phase2b(a_ballot, slot));
    if (!phase2b_record.containsKey(slot) || !phase2b_record.get(slot).contains(pair)) {
      phase2b_record.put(slot, pair);
    }
    Logger.getLogger("").info("Finish PROPOSE by: "+ this.address+" , "+num );

    //update phase2b_record???
    List<Pair<Integer, Pair<Address, Phase2b>>> toRemove = new ArrayList<>();
    for(Integer slots:phase2b_record.keySet()) {
      for (Pair<Address, Phase2b> pairs : phase2b_record.get(slots)){
        if(pairs.getRight().ballot_num()<l_ballot){
          toRemove.add(Pair.of(slots, pairs));
        }
      }
    }
    for (Pair<Integer, Pair<Address, Phase2b>> removal : toRemove) {
      phase2b_record.remove(removal.getLeft(), removal.getRight());
    }

  }
  private void perform(Decision decision){//server deal with decision
    //pretend to receive decision (as self.replica)
    AMOCommand comm=decision.com();
    decisions.put(decision.slot(),comm);
    //state.put(decision.slot(),comm);//update state
    //Logger.getLogger("State").info("State of: " +this.address+", "+ state);

    while(decisions.containsKey(slot_out)){//execute
      if(!Objects.equals(application,null)){
        result=application.execute(decisions.get(slot_out));
        //results.put(decisions.get(slot_out),result);//record <AMOCommand,result>
        requests.remove(AMOResult.getAddress(result));//delete requests
        if(isActive){
          send(new PaxosReply(result),AMOResult.getAddress(result));
        }
      }
      else{//subnode message won't drop
        handleMessage(new PaxosRequest(decisions.get(slot_out)),sendAdd);//send back next AMOCommand
      }
      slot_out++;
    }
    Logger.getLogger("").info("Finish perform by: "+this.address );
    Logger.getLogger("").info("slot_out of: "+this.address +" = "+slot_out);
  }
  private void garbage_collect(Integer num){
    ///start garbage collection!!!!!
    for(int i=num;i>0;i--){
      if(!decisions.containsKey(i)&&!accepted.containsKey(i)
      &&!phase2b_record.containsKey(i)&&!l_proposals.containsKey(i)){
        break;
      }
      decisions.remove(i);
      accepted.remove(i);
      phase2b_record.removeAll(i);
      l_proposals.remove(i);
    }
    if(l_proposals.isEmpty()){//no proposal anymore
      phase1b_record.clear();
      statistic.clear();
    }
    Logger.getLogger("").info("Finish garbage collection by: "+ this.address +",cleared: "+num);
  }
  private void findSlot_in(){
    for(int i=slot_out;i<Integer.MAX_VALUE;i++){
      if(!decisions.containsKey(i)&&!accepted.containsKey(i)){
        slot_in=i;
        return;
      }
    }
  }
  private int findMaxKey(HashMap<Integer, ?> map) {
    int maxKey = Integer.MIN_VALUE;
    for (int key : map.keySet()) {
      if (key > maxKey) {
        maxKey = key;
      }
    }
    return maxKey;
  }
  @Data
  final static class pvalue implements Serializable {
    private final double ballot_num;
    private final int slot_num;
    private final AMOCommand com;
  }
}


