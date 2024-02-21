package dslabs.primarybackup;

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

//self-adding
import dslabs.primarybackup.ViewReply;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class PBClient extends Node implements Client {
  private final Address viewServer;

  // Your code here...
  private final Address clientAddress;
  private View currentView;
  private Address currentPrimary;
  int sequenceNum=0;//initialize
  public AMOCommand comm;
  private AMOResult res;
  private Request req;
  HashMap<Address,Integer> map2=new HashMap< Address,Integer>();//clientAdd,sequenceNum
  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public PBClient(Address address, Address viewServer) {
    super(address);
    this.viewServer = viewServer;
    this.clientAddress=address;
  }

  @Override
  public synchronized void init() {
    // Your code here...
    send(new GetView(),viewServer);
    //set(ClientTimer(),ClientTimer.CLIENT_RETRY_MILLIS);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Client Methods
   * ---------------------------------------------------------------------------------------------*/
  @Override
  public synchronized void sendCommand(Command command) {
    // Your code here...
    if(Objects.equal(currentPrimary,null)){
      send(new GetView(),viewServer);
      Address pastCurrentPrimary=this.currentPrimary;
      set(new RetryTimer(pastCurrentPrimary),RetryTimer.RETRY_MILLIS);
    }

      Command com = (Command) command;
      sequenceNum++;
      this.comm = new AMOCommand(com, sequenceNum, clientAddress);
      req = new Request(comm);
      this.send(req, currentPrimary);
      this.set(new ClientTimer(comm), ClientTimer.CLIENT_RETRY_MILLIS);

  }

  @Override
  public synchronized boolean hasResult() {
    // Your code here...
    return Objects.equal(map2.get(clientAddress),AMOCommand.getSequenceNum(comm));
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
  private synchronized void handleReply(Reply m, Address sender) {
    // Your code here...
    res=Reply.getResult(m);//AMOResult
    //already handled
    if (Objects.equal(map2.get(AMOResult.getAddress(res)),AMOResult.getSequenceNum(res)))return;

    if (Reply.getTrue(m)){
      map2.put(AMOResult.getAddress(res),AMOResult.getSequenceNum(res));
      this.notify();
    }
    else{//receive error from primary, getView()
      send(new GetView(),viewServer);
      set(new RetryTimer(sender),RetryTimer.RETRY_MILLIS);
    }

  }

  private synchronized void handleViewReply(ViewReply m, Address sender) {
    // Your code here...
    currentView=ViewReply.getView(m);
    currentPrimary=currentView.getPrimary();
    this.notify();//keep!
  }

  // Your code here...

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void onClientTimer(ClientTimer t) {
    // Your code here...
    //set(new RetryTimer(currentPrimary),RetryTimer.RETRY_MILLIS);//error when primary actually not dead
    AMOCommand comm=ClientTimer.getCommand(t);
    //received result already
    if(map2.containsKey(AMOCommand.getAddress(comm))&& (map2.get(AMOCommand.getAddress(comm))>=AMOCommand.getSequenceNum(comm)))return;
    send(new GetView(),viewServer);//current primary seems to be dead
    send(new Request(comm),currentPrimary);
    set(t,ClientTimer.CLIENT_RETRY_MILLIS);
  }
  //self-adding
  private synchronized void onRetryTimer(RetryTimer t){
    Address pastPrimaryAdd=RetryTimer.getPastPrimary(t);
    if(!Objects.equal(currentPrimary,pastPrimaryAdd))return;//received new PrimaryAdd
    send(new GetView(), viewServer);
    set(new RetryTimer(pastPrimaryAdd), RetryTimer.RETRY_MILLIS);
  }
}
