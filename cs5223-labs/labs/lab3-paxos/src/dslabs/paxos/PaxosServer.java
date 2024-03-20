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
import java.util.ArrayList;
import java.util.HashMap;
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
  private boolean OneServer=false;
  private final Application app;
  private final AMOApplication application;
  private int cleared=0;//max cleared log num
  private HashMap<Integer,AMOCommand> decisions =new HashMap<>();//set of decisions
  private HashMap<Address,PaxosRequest> requests =new HashMap<>();//set of requests
  //private HashMap<AMOCommand,AMOResult> results =new HashMap<>();//record results
  private int slot_in =1;//next propose
  private int slot_out =1;//next execute
  private AMOResult result;
  //////acceptors
  private double a_ballot=0;
  private HashMap<Integer,pvalue> accepted =new HashMap<>();//{slot_num:pvalues<b,s,c>,...}
  //////leaders
  private int leader_number;//this.number
  private int seq_num=0;//seqnum
  private boolean isActive=false;//is active leader
  private boolean election=false;//is in process of election
  private double l_ballot=0;
  private boolean dis_alive_f=false;
  private boolean dis_alive_s=false;
  private  boolean stop_phase1a_timer=false;
  private HashMap<Integer,AMOCommand> l_proposals =new HashMap<>();//set of proposals
  private HashMap<Address, Phase1b> phase1b_record=new HashMap<>();//record each server'phase1b reply
  private Multimap<Integer,Pair<Address, Phase2b>> phase2b_record= ArrayListMultimap.create();//record each server'phase2b reply
  private HashMap<Address,Integer> statistic=new HashMap<>();//count heartbeatReply and decide cleared




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

  @Override
  public void init() {
    // Your code here...
    if(servers.length==1){//one-server
      OneServer=true;
    }
    else{
      for (int i=0;i<servers.length;i++){//generate leader_num
        if (Objects.equals(servers[i],this.address)) {
          leader_number = i;
          break;
        }
      }
      double num=seq_num+0.1*leader_number;
      l_ballot=num;
      elect(num);
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
    AMOCommand comm = m.command;
//    if(results.containsKey(comm)){//active_leader got results of m.command
//      send(new PaxosReply(results.get(comm)), sender);
//      return;
//    }
    if (OneServer) {
      isActive=true;
      requests.put(sender,m);
      accepted.put(slot_in,new pvalue(l_ballot,slot_in,comm));
      perform(new Decision(slot_in,comm));
      slot_in++;
    }
    else {
      //all leaders save requests{address:request}
      if (!requests.containsKey(sender) || requests.get(sender) != m) {
        requests.put(sender, m);
      }
      //distinguished leader propose,and save in l_proposals{slot:command,...}
      if (isActive) {
        if (!l_proposals.containsValue(comm)) {//never proposed
          findSlot_in();
          l_proposals.put(slot_in, comm);
          propose(l_ballot,slot_in,comm);

        }
//        else {//has proposed and re-propose
//          for (Integer num : l_proposals.keySet()) {
//            if (Objects.equals(l_proposals.get(num), comm)) {
//              //propose(l_ballot,num,comm);
//            }
//          }
//        }
      }
    }
  }

  // Your code here...

  private void handlePhase1a(Phase1a message,Address sender){
    //AS Acceptors
    if(a_ballot<message.ballot_num()){
      a_ballot=message.ballot_num();
    }
    send(new Phase1b(a_ballot,accepted),sender);
    //Logger.getLogger("SendP1b in Phase1a").info("SendP1b(b,accepted): " + a_ballot+", "+accepted);
  }
  private void handlePhase1b(Phase1b message,Address sender){
    //AS Leaders
    //Logger.getLogger("P1b").info("P1b(b,accepted): " + message.ballot_num()+", "+message.accepted());
    if(phase1b_record.containsKey(sender)//old messages
        &&(message.ballot_num()<=phase1b_record.get(sender).ballot_num())) {return;}
    phase1b_record.put(sender,message);
    count_1(phase1b_record);
    if(isActive) {//new Dleader:l_proposals should update, and propose
      //if p1b.accepted is not null, then propose the largest ballot_num command
      HashMap<Integer, pvalue> newaccepted= new HashMap<>();//{slot_num:pvalue,...}
      for (Phase1b p1b : phase1b_record.values()) {//build newaccepted
        if (!Objects.equals(p1b.accepted(), null)) {//accepted is not null
          for (Integer slot_num : p1b.accepted().keySet()) {
            pvalue pv = p1b.accepted().get(slot_num);
            if (!newaccepted.containsKey(slot_num)
                || newaccepted.get(slot_num).ballot_num < pv.ballot_num) {
              newaccepted.put(slot_num, pv);
            }
          }
        }
      }
      for(Integer slot: newaccepted.keySet()){//prioritize accept newaccepted
        AMOCommand comm=newaccepted.get(slot).com;
        l_proposals.put(slot,comm);
        propose(l_ballot,slot,comm);
      }
      //merge l_proposals with requests and propose
      for(PaxosRequest request:requests.values()){//combine with requests
        AMOCommand comm=request.command;
        if(!l_proposals.containsValue(comm)){
          findSlot_in();
          int slot=slot_in;
          l_proposals.put(slot,comm);
          propose(l_ballot,slot,comm);
        }
      }
      ArrayList<Integer> keysToRemove = new ArrayList<>();//remove old proposals(<slot_out)
      for (Integer num : l_proposals.keySet()) {
        if (num < slot_out) {
          keysToRemove.add(num);
        }
      }
      for (Integer key : keysToRemove) {
        l_proposals.remove(key);
      }
      //Logger.getLogger("NEW DISTINGUISHED Leader").info("New distinguished leader: " + this.address+"ballot_num:  "+l_ballot);

    }
}

  private void handlePhase2a(Phase2a message,Address sender){
    //AS Acceptors
    //sender think it is active leader
    if (Objects.equals(message.ballot_num(),a_ballot)){
      pvalue pv=new pvalue(message.ballot_num(),message.slot_num(),message.com());
      accepted.put(message.slot_num(),pv);
    }
    send(new Phase2b(a_ballot,message.slot_num()),sender);
    //Logger.getLogger("Message").info(this.address+" Send P2b Message(b,s): " + a_ballot+", "+message.slot_num());
  }

  private void handlePhase2b(Phase2b message,Address sender){

    if (!isActive){return;}//As Distinguished Leaders
    // Logger.getLogger("Receive P2b").info("P2b(s,b): " + message.slot_num()+", "+message.ballot_num());
    Integer slot_num=message.slot_num();
    Pair<Address, Phase2b> pair = Pair.of(sender, message);
    if (!phase2b_record.containsKey(slot_num) || !phase2b_record.get(slot_num).contains(pair)) {
      phase2b_record.put(slot_num, pair);

    }
    count_2(phase2b_record);
  }
  private void handleDecision(Decision message,Address sender){
    if(!decisions.containsKey(message.slot())){//havn't performed
      perform(message);
    }
  }
  private void handleHeartbeat(Heartbeat message,Address sender){
    dis_alive_f=true;

    //stop_phase1a_timer=true;
    cleared=message.cleared();
    send(new HeartbeatReply(slot_out),sender);
    garbage_collect(cleared);
  }
  private void handleHeartbeatReply(HeartbeatReply message,Address sender){
    //distinguished leader
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
      }
      statistic.clear();
      garbage_collect(cleared);
    }
    int num=message.slot_out();
    if(num<slot_out){//the server is out of date, need to resend decision to it
      send(new Decision(num,decisions.get(num)),sender);
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
      send(new Heartbeat(cleared), add);
    }
  }
  set(t,HeartBeatTimer.RETRY_MILLIS);
}

private void onCheckActive(CheckActive t){

  if(isActive)return;//only follower leader need check active
  if(election)return;//in process of election
  if(!dis_alive_f&&!dis_alive_s){//current distinguished leader dead
    seq_num++;
    double num=seq_num+0.1*leader_number;
    l_ballot=num;
    elect(num);
    election=true;

  }
  set(t,CheckActive.RETRY_MILLIS);
  Logger.getLogger("").info("FinishCheckActiveTimer by: "+this.address+ " leader_alive: "+ (dis_alive_s||dis_alive_f));
  dis_alive_s=dis_alive_f;
  dis_alive_f=false;
}
private void onPhase1aTimer(Phase1aTimer t){
  //AS Leaders
  if(isActive){return;}

  if(stop_phase1a_timer){return;}
  //phase1b_record.clear();
  elect(t.num());
  election=true;

}

private void onPhase2aTimer(Phase2aTimer t){
  //AS Leaders

  if(!isActive){return;}
  if(decisions.containsKey(t.slot_num())){return;}
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
  if (counts.containsKey("P") && counts.get("P") > servers.length / 2) {//received majority
    isActive = false;
    stop_phase1a_timer = true;
    election=false;
    set(new CheckActive(),CheckActive.RETRY_MILLIS);
  }
  else if (counts.containsKey("D") && counts.get("D") > servers.length / 2) {
    isActive = true;//New leader
    stop_phase1a_timer = true;
    election=false;
    //broadcast HeartBeat to all leaders
    for (Address add : servers) {
      if (!add.equals(this.address)) {
        send(new Heartbeat(cleared), add);
      }
    }
    set(new HeartBeatTimer(),HeartBeatTimer.RETRY_MILLIS);//resend broadcast
    Logger.getLogger("leader").info("Distinguished: " + this.address+" ballot_num: "+l_ballot);
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
          Logger.getLogger("leader").info( "Preempted"+this.address);
          isActive = false;
          set(new CheckActive(), CheckActive.RETRY_MILLIS);
        } else if (counts.containsKey("D") && counts.get("D") > servers.length / 2) {//Distinguished
          isActive = true;
          if (l_proposals.containsKey(slot_num)&&!decisions.containsKey(slot_num)) {//send decision of slot_num
            AMOCommand comm = l_proposals.get(slot_num);
            Decision decision = new Decision(slot_num, comm);
            Logger.getLogger("Decision").info("Decision: " + decision);
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
        send(new Phase1a(num),add);
      }
    }
    stop_phase1a_timer=false;
    set(new Phase1aTimer(num),Phase1aTimer.RETRY_MILLIS);//resend p1a until active_leader
    //pretend to handle phase1a as self.acceptor
    if (a_ballot < l_ballot) {//repeated
      a_ballot = l_ballot;
    }
    //pretend to receive phase1b as self.leader
    phase1b_record.put(this.address,new Phase1b(a_ballot,accepted));
    Logger.getLogger("").info("Finish ELECT by: "+this.address );
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
    Logger.getLogger("").info("Finish PROPOSE by: "+ this.address );
  }
  private void perform(Decision decision){//server deal with decision
    //pretend to receive decision (as self.replica)
    AMOCommand comm=decision.com();
    decisions.put(decision.slot(),comm);
    //state.put(decision.slot(),comm);//update state
    //Logger.getLogger("State").info("State of: " +this.address+", "+ state);

    while(decisions.containsKey(slot_out)){//execute
      result=application.execute(decisions.get(slot_out));
      //results.put(decisions.get(slot_out),result);//record <AMOCommand,result>
      //requests.remove(AMOResult.getAddress(result));//delete requests
      if(isActive){
        send(new PaxosReply(result),AMOResult.getAddress(result));
      }
      slot_out++;
    }
    Logger.getLogger("").info("Finish perform by: "+this.address );
    Logger.getLogger("").info("slot_out of: "+this.address +" = "+slot_out);
  }
  private void garbage_collect(Integer num){
    ///start garbage collection!!!!!
    for(int i=1;i<num+1;i++){

      decisions.remove(i);
      accepted.remove(i);
      phase2b_record.removeAll(i);
    }
    Logger.getLogger("").info("Finish garbage collection by: "+ this.address );
  }
  private void findSlot_in(){
    for(int i=slot_out;i<Integer.MAX_VALUE;i++){
      if(!decisions.containsKey(i)&&!accepted.containsKey(i)){
        slot_in=i;
        return;
      }
    }
  }
  private static int findMaxKey(HashMap<Integer, ?> map) {
    int maxKey = Integer.MIN_VALUE;
    for (int key : map.keySet()) {
      if (key > maxKey) {
        maxKey = key;
      }
    }
    return maxKey;
  }
  @Data
  final static class pvalue{
    private final double ballot_num;
    private final int slot_num;
    private final AMOCommand com;
  }
}


