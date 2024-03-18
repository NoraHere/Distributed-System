package dslabs.paxos;


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
import lombok.EqualsAndHashCode;
import lombok.ToString;
import java.util.logging.Logger;//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)

public class PaxosServer extends Node {
  /** All servers in the Paxos group, including this one. */
  private final Address[] servers;

  // Your code here...
  private final Address address;//this address
  private boolean OneServer=false;
  private Application app;
  private AMOApplication application;
  private int cleared=0;//max cleared log num
  ///////replicas
  private HashMap<Integer,AMOCommand> state=new HashMap<>();//applictaion state
  private int slot_in =1;
  private int slot_out =1;
  private AMOResult result;
  private HashMap<Address,PaxosRequest> requests =new HashMap<>();//set of requests

  private HashMap<Decision,AMOResult> results =new HashMap<>();//record results

  //////acceptors
  private double a_ballot=0;
  private ArrayList<pvalue> accepted =new ArrayList<>();//set of pvalues<b,s,c>

  //////leaders
  private int leader_number;//this.number
  private int seq_num=0;//seqnum
  private boolean isActive=false;//is active leader
  private boolean adopted=false;//adopted the proposal
  private double l_ballot=0;
  private boolean dis_alive_f=false;
  private boolean dis_alive_s=false;
  private  boolean stop_phase1a_timer=false;
  private  boolean stop_phase2a_timer=false;
  private HashMap<Integer,AMOCommand> l_proposals =new HashMap<>();//set of proposals
  private HashMap<Address, Phase1b> phase1b_record=new HashMap<>();//record each server'phase1b reply
  private HashMap<Address,Phase2b> phase2b_record=new HashMap<>();//record each server'phase2b reply
  private HashMap<Integer,AMOCommand> decisions =new HashMap<>();//set of decisions




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
      for (int i=0;i<servers.length;i++){
        if (Objects.equals(servers[i],this.address)) {
          leader_number = i;
          break;
        }
      }
      //get elected
      //Leaders
      double num=seq_num+0.1*leader_number;
      l_ballot=num;
      for (Address add : servers){
        if (! add.equals(this.address)){//send phase1a to all acceptors
          send(new Phase1a(num),add);
        }
      }
      set(new Phase1aTimer(num),Phase1aTimer.RETRY_MILLIS);//resend p1a until active_leader

      if(!phase1b_record.containsKey(this.address)){//pretend to handle phase1a as self.acceptor
        if(a_ballot<l_ballot){//repeated
          a_ballot=l_ballot;
        }
        phase1b_record.put(this.address,new Phase1b(a_ballot,accepted));
      }

      if(!isActive){
        set(new CheckActive(),CheckActive.RETRY_MILLIS);//check if active leader still active
      }
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
    if(logSlotNum>=slot_out && logSlotNum<slot_in){
      return PaxosLogSlotStatus.ACCEPTED;
    }
    else if(logSlotNum<slot_out && logSlotNum>cleared){
      return PaxosLogSlotStatus.CHOSEN;
    }
    //?????Garbage Collection
    else if(logSlotNum<=cleared) {
      return PaxosLogSlotStatus.CLEARED;
    }
    else if(logSlotNum>=slot_in){
      return PaxosLogSlotStatus.EMPTY;
    }
    else {return null;}
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
    if(status(logSlotNum)==PaxosLogSlotStatus.EMPTY||status(logSlotNum)==PaxosLogSlotStatus.CLEARED){
      return null;
    }
    else if(!state.containsKey(logSlotNum)){
      return null;
    }
    else{
      return AMOCommand.getCommand(state.get(logSlotNum));
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
      return slot_in-1;

  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void handlePaxosRequest(PaxosRequest m, Address sender) {
    // Your code here...
    //As Leaders
    AMOCommand comm = (AMOCommand) m.command;

    //check results ??already make decision
    for (Decision decision : results.keySet()) {
      if (decision.com.equals(comm)) {
        send(new PaxosReply(results.get(decision)), sender);//send client response
        return;
      }
    }

    if (OneServer) {
      state.put(slot_in, comm);
      Decision decision = new Decision(slot_in, comm);
      result = application.execute(comm);
      results.put(decision, result);//record <decision,result>
      slot_in++;
      slot_out = slot_in;
      send(new PaxosReply(result), sender);
    }
    else {
      //save in requests{add:request}
      if (!requests.containsKey(sender)
          || requests.get(sender) != m) {
        requests.put(sender, m);
      }

      if (isActive) {//distinguished leader propose,and save in l_proposals
        if (!l_proposals.containsValue(comm)) {
          l_proposals.put(slot_in,comm);
          for (Address add : servers) {//send phase2a to all acceptors
            if (!add.equals(this.address)) {
              send(new Phase2a(l_ballot,slot_in,comm), add);//<b,s,c>
            }
          }
          set(new Phase2aTimer(l_ballot,slot_in,comm),Phase1aTimer.RETRY_MILLIS);
          //pretend to handle phase2a as self.acceptor
          if(Objects.equals(a_ballot,l_ballot)){
            pvalue pv=new pvalue(l_ballot,slot_in,comm);
            accepted.add(pv);
          }
          //pretend to receive phase2b as self.dleader
          phase2b_record.put(this.address,new Phase2b(a_ballot,slot_in));
          slot_in++;
        }
      }
    }
  }

  // Your code here...

  private void handlePhase1a(Phase1a message,Address sender){
    //AS Acceptors
    if(a_ballot<message.ballot_num){
      a_ballot=message.ballot_num;
    }
    send(new Phase1b(a_ballot,accepted),sender);
  }
  private void handlePhase1b(Phase1b message,Address sender){
    //AS Leaders
    //old messages
    if(phase1b_record.containsKey(sender)&&(message.ballot_num<phase1b_record.get(sender).ballot_num)){
      return;
    }
    phase1b_record.put(sender,message);
    count_1(phase1b_record);//Statistic
    if(isActive){
      Logger.getLogger("").info("Distinguished: " + this.address);
      //if accepted is not null, then propose the largest ballot_num command
      if(!Objects.equals(message.accepted,null) && !message.accepted.isEmpty()){
        pvalue propose=message.accepted.get(0);
        for (Phase1b mess: phase1b_record.values()){//all accepted[]
          if(mess.accepted.get(0).ballot_num>propose.ballot_num){
            propose=mess.accepted.get(0);
          }
        }
        for (Address add : servers) {//send p2a to all acceptors
          if (!add.equals(this.address)) {
            send(new Phase2a(l_ballot,propose.slot_num,propose.com), add);
          }
        }
        set(new Phase2aTimer(l_ballot,propose.slot_num,propose.com),Phase2aTimer.RETRY_MILLIS);

        //handle p2a as self.acceptor
        if (propose.ballot_num==a_ballot){
          accepted.add(propose);
        }
        //pretend to accept phase2b from self.leader
        if(!(phase2b_record.containsKey(this.address)&&(a_ballot<=phase2b_record.get(this.address).ballot_num))){
          phase2b_record.put(this.address,new Phase2b(a_ballot,propose.slot_num));
          count_2(phase2b_record);
        }
      }
      phase1b_record.clear();
      //broadcast HeartBeat to all leaders
      for (Address add : servers) {
        if (!add.equals(this.address)) {
          send(new Heartbeat(), add);
        }
      }
      set(new HeartBeatTimer(),HeartBeatTimer.RETRY_MILLIS);//resend broadcast
    }
  }

  private void handlePhase2a(Phase2a message,Address sender){
    //AS Acceptors
    //sender is active leader
    if (message.ballot_num==a_ballot){
      pvalue pv=new pvalue(message.ballot_num,message.slot_num,message.com);
      accepted.add(pv);
    }
    send(new Phase2b(a_ballot,message.slot_num),sender);//???slot num
    Logger.getLogger("Message").info("P2b Message(b,s): " + a_ballot+", "+message.slot_num);
  }

  private void handlePhase2b(Phase2b message,Address sender){
    //As Distinguished Leaders
    //old messages
    if (!isActive){return;}
    if(phase2b_record.containsKey(sender)&&(message.ballot_num<=phase2b_record.get(sender).ballot_num)){
      return;
    }
    if(message.slot_num<slot_out){return;}

    phase2b_record.put(sender,message);
    count_2(phase2b_record);//Statistic

    if(adopted){
      phase2b_record.clear();
      //send decision to all replicas
      Logger.getLogger("l_proposals").info("l_proposals of "+this.address +", "+ l_proposals);
      AMOCommand comm=l_proposals.get(message.slot_num);
      Decision decision=new Decision(message.slot_num,comm);
      for (Address add : servers) {
        if (!add.equals(this.address)) {
          send(decision, add);
        }
      }
      Logger.getLogger("Decision").info("Command in Decision: " + comm);

      //pretend to receive decision (as self.replica)
      state.put(decision.slot,decision.com);
      Logger.getLogger("State").info("State of: " +this.address+", "+ state);
      slot_out=message.slot_num+1;
      result=application.execute(comm);
      results.put(decision,result);//record <decision,result>
      requests.remove(AMOCommand.getAddress(decision.com));//remove client add

      //send PaxosReply to client as Dleader
      decisions.put(message.slot_num,comm);
      Logger.getLogger("Decisions").info("Decisions: " + decisions);
      send(new PaxosReply(result),AMOCommand.getAddress(decision.com));//send to client
      Logger.getLogger("").info("Decision: " + decisions.get(message.slot_num));
      l_proposals.remove(message.slot_num);





//      if(!l_proposals.isEmpty()){
//        //propose the smallest slot_num l_proposals
//        Integer smallest_slot=null;
//        for(Integer integer:l_proposals.keySet()){
//          if (smallest_slot==null||integer.compareTo(smallest_slot)<0){
//            smallest_slot=integer;
//          }
//        }
//        pvalue pv = new pvalue(l_ballot, smallest_slot, l_proposals.get(smallest_slot));
//        for (Address add : servers) {//send phase2a to all acceptors
//          if (!add.equals(this.address)) {
//            send(new Phase2a(pv), add);
//          }
//        }
//        set(new Phase2aTimer(pv), Phase2aTimer.RETRY_MILLIS);//retry when no enough message received
//      }

    }
  }
  private void handleDecision(Decision message,Address sender){
    //AS Replicas
    AMOCommand comm=(AMOCommand) message.com;
    slot_out=message.slot+1;
    state.put(message.slot,comm);
    Logger.getLogger("State").info("State of: " +this.address+", "+ state);
    slot_in=slot_out+1;
    Logger.getLogger("slot_in/slot_out").info("Slot_in of: " +this.address+", "+ slot_in+", Slot_out: "+slot_out);
    result=application.execute(comm);
    results.put(message,result);//record <decision,result>
    requests.remove(AMOCommand.getAddress(comm));//remove client
  }
  private void handleHeartbeat(Heartbeat message,Address sender){
    dis_alive_f=true;
    stop_phase1a_timer=true;
  }


  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  // Your code here...
private void onHeartBeatTimer(HeartBeatTimer t){
  if(isActive){
    for (Address add : servers) {//send HeartBeat to all leaders
      if (!add.equals(this.address)) {
        send(new Heartbeat(), add);
      }
    }
    set(t,HeartBeatTimer.RETRY_MILLIS);
  }
}

private void onCheckActive(CheckActive t){
  if(!isActive){
    if(!dis_alive_f && !dis_alive_s){//current distinguished leader dead
      seq_num++;
      double num=seq_num+0.1*leader_number;
      l_ballot=num;
      for (Address add : servers){
        if (! add.equals(this.address)){//send phase1a to all acceptors
          send(new Phase1a(num),add);
        }
      }
      if(!phase1b_record.containsKey(this.address)){//pretend to accept phase1b from self
        if(a_ballot<num){
          a_ballot=num;
        }
        phase1b_record.put(this.address,new Phase1b(a_ballot,accepted));
      }
      set(new Phase1aTimer(num),Phase1aTimer.RETRY_MILLIS);//resend p1a until active_leader
    }
    set(t,CheckActive.RETRY_MILLIS);
    dis_alive_s=false;
    dis_alive_s=dis_alive_f;
    dis_alive_f=false;
  }
}
private void onPhase1aTimer(Phase1aTimer t){
  //AS Leaders
  if(isActive){return;}
  if(stop_phase1a_timer){
    stop_phase1a_timer=false;
    return;
  }

  for (Address add : servers){//resend if no received enough phase1b message
    if (! add.equals(this.address)){//send phase1a to all acceptors
      send(new Phase1a(t.num),add);
    }
  }
  if(!phase1b_record.containsKey(this.address)){//pretend to handle phase1a as self.acceptor
    if(a_ballot<l_ballot){//repeated
      a_ballot=l_ballot;
    }
    phase1b_record.put(this.address,new Phase1b(a_ballot,accepted));
  }
  set(t,Phase1aTimer.RETRY_MILLIS);
}

//private void onProposalTimer(ProposalTimer t){
//  //AS Replicas
//  int slot_num=t.slot_num;
//  AMOCommand comm=t.com;
//  if(Objects.equals(state,null)||!state.values().contains(comm)){//don't receive decision of the command
//    for (Address add : servers){
//      if (! add.equals(this.address)){//send Proposals to all leaders
//        slot_num=slot_in;
//        slot_in++;
//        send(new Proposal(slot_num,t.com),add);
//      }
//    }
//  }
//  else{return;}
//}

private void onPhase2aTimer(Phase2aTimer t){
  //AS Leaders
  if(stop_phase2a_timer){
    stop_phase2a_timer=false;
    return;
  }
  for (Address add : servers){
    if (! add.equals(this.address)){//send phase1a to all acceptors
      send(new Phase2a(t.ballot_num,t.slot_num,t.com),add);
    }
  }

  //pretend to handle phase2a as self.acceptor
  if(Objects.equals(a_ballot,l_ballot)){
    pvalue pv=new pvalue(a_ballot,t.slot_num,t.com);
    accepted.add(pv);
  }
  //pretend to receive phase2b as self.dleader
  phase2b_record.put(this.address,new Phase2b(a_ballot,t.slot_num));

  set(t,Phase2aTimer.RETRY_MILLIS);
}
  /* -----------------------------------------------------------------------------------------------
   *  Utils
   * ---------------------------------------------------------------------------------------------*/
  // Your code here...
public void count_1(HashMap<Address, Phase1b> record) {
  //Statistic
  HashMap<Address, String> count1 = new HashMap<>();
  HashMap<String, Integer> counts = new HashMap<>();//count majority

  for (Address add : record.keySet()) {
    //phase1b
    if (((Phase1b) record.get(add)).ballot_num == l_ballot) {//Distinguished
      count1.put(add, "D");
    } else {//Preempted
      count1.put(add, "P");
    }
    for (String value : count1.values()) {//count
      counts.put(value, counts.getOrDefault(value, 0) + 1);
    }
    if (counts.containsKey("P") && counts.get("P") > servers.length / 2) {//received majority
      isActive = false;
      stop_phase1a_timer = true;
    } else if (counts.containsKey("D") && counts.get("D") > servers.length / 2) {
      isActive = true;
      stop_phase1a_timer = true;
      Logger.getLogger("").info("Distinguished: " + this.address);
    }
  }
}

  public void count_2(HashMap<Address, Phase2b> record) {
    //Statistic
    HashMap<Address, String> count1 = new HashMap<>();
    HashMap<String, Integer> counts = new HashMap<>();//count majority

    for (Address add : record.keySet()) {
      //phase1b
      if (((Phase2b) record.get(add)).ballot_num == l_ballot) {//Distinguished
        count1.put(add, "D");
      } else {//Preempted
        count1.put(add, "P");
      }
      for (String value : count1.values()) {//count
        counts.put(value, counts.getOrDefault(value, 0) + 1);
      }
      if (counts.containsKey("P") && counts.get("P") > servers.length / 2) {//received majority
        adopted = false;
        isActive=false;
        stop_phase2a_timer = true;
      } else if (counts.containsKey("D") && counts.get("D") > servers.length / 2) {
        adopted = true;
        isActive=true;
        stop_phase2a_timer = true;
      }
    }
  }

  public static class pvalue{
    double ballot_num;
    int slot_num;
    AMOCommand com;
    public pvalue(double ballot_num,int slot_num,AMOCommand com){
      this.ballot_num=ballot_num;
      this.slot_num=slot_num;
      this.com=com;
    }
  }
}


