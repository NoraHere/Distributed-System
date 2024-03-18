package dslabs.paxos;

import com.google.common.base.Objects;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Result;
import java.util.HashMap;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public final class PaxosClient extends Node implements Client {
  private final Address[] servers;

  // Your code here...
  private final Address clientaddress;
  public AMOCommand comm;
  private AMOResult res;
  int sequenceNum=0;//initialize
  private PaxosRequest req;
  HashMap<Address,Integer> map2=new HashMap< Address,Integer>();//record (Address,seqNum)

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public PaxosClient(Address address, Address[] servers) {
    super(address);
    this.servers = servers;
    this.clientaddress=address;
  }

  @Override
  public synchronized void init() {
    // No need to initialize
  }

  /* -----------------------------------------------------------------------------------------------
   *  Client Methods
   * ---------------------------------------------------------------------------------------------*/
  @Override
  public synchronized void sendCommand(Command operation) {
    // Your code here...
    //Command com = (Command) operation;
    sequenceNum++;
    this.comm= new AMOCommand(operation,sequenceNum,this.clientaddress);
    req=new PaxosRequest(comm);
    for(Address add:servers){//send to all replicas
      this.send(req,add);
    }
    this.set(new ClientTimer(comm),ClientTimer.CLIENT_RETRY_MILLIS);
  }

  @Override
  public synchronized boolean hasResult() {
    // Your code here...
    return Objects.equal(map2.get(clientaddress),AMOCommand.getSequenceNum(comm));
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
   * Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void handlePaxosReply(PaxosReply m, Address sender) {
    // Your code here...
    res = m.result;
    if (Objects.equal(map2.get(AMOResult.getAddress(res)),AMOResult.getSequenceNum(res)))return;
    map2.put(AMOResult.getAddress(res),AMOResult.getSequenceNum(res));
    this.notify();
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void onClientTimer(ClientTimer t) {
    // Your code here...
    AMOCommand comm=t.command;
    if(map2.containsKey(AMOCommand.getAddress(comm))&& (map2.get(AMOCommand.getAddress(comm))>=AMOCommand.getSequenceNum(comm)))return;
    for(Address add:servers){
      this.send(new PaxosRequest(comm),add);
      this.set(t,ClientTimer.CLIENT_RETRY_MILLIS);
    }
  }
}
