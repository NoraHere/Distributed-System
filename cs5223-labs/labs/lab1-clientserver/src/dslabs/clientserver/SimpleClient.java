package dslabs.clientserver;

import com.google.common.base.Objects;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Result;

import lombok.EqualsAndHashCode;
import lombok.ToString;

//self adding
import static dslabs.clientserver.ClientTimer.CLIENT_RETRY_MILLIS;
import java.util.HashMap;
import dslabs.kvstore.KVStore.Get;
import dslabs.kvstore.KVStore.Put;
import dslabs.kvstore.KVStore.Append;
import dslabs.kvstore.KVStore.KVStoreResult;
import dslabs.clientserver.Request;
import dslabs.kvstore.KVStore.SingleKeyCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.atmostonce.AMOCommand;
import dslabs.clientserver.ClientTimer;
/**
 * Simple client that sends requests to a single server and returns responses.
 *
 * <p>See the documentation of {@link Client} and {@link Node} for important implementation notes.
 */
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class SimpleClient extends Node implements Client {
  private final Address serverAddress;

  // Your code here...
  private final Address clientaddress;
  public AMOCommand comm;
  private AMOResult res;
  int sequenceNum=0;//initialize
  private Request req;
  HashMap<Address,Integer> map2=new HashMap< Address,Integer>();
  //HashMap<Address,AMOResult> map2=new HashMap< Address,AMOResult>();
  ///HashMap<Address,Integer> map4=new HashMap< Address,Integer>();

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public SimpleClient(Address address, Address serverAddress) {
    super(address);
    this.serverAddress = serverAddress;
    //adding
    this.clientaddress = address;
  }

  @Override
  public synchronized void init() {
    // No initialization necessary
  }

  /* -----------------------------------------------------------------------------------------------
   *  Client Methods
   * ---------------------------------------------------------------------------------------------*/
  @Override
  public synchronized void sendCommand(Command command) {
    // Your code here...
    //if (!(command instanceof Put ||command instanceof Get||command instanceof Append)) {
     // throw new IllegalArgumentException();
    //}
    Command com = (Command) command;
    sequenceNum++;
    this.comm= new AMOCommand(com,sequenceNum,clientaddress);
    //new (com,sequenceNum,clientaddress);

    //comm.command()=command;
    //comm.sequenceNum()=sequenceNum;
    //comm.address()=serverAddress;
    //comm=new AMOCommand;
    req=new Request(comm);
    this.send(req,serverAddress);
    this.set(new ClientTimer(comm),CLIENT_RETRY_MILLIS);
    //map2.put(sequenceNum,req);



  }

  @Override
  public synchronized boolean hasResult() {
    // Your code here...
    return Objects.equal(map2.get(clientaddress),comm.sequenceNum());
    //return (map2.containsKey(req.command().address())&&map2.get(req.command().address())>=req.command().sequenceNum());
    ///return map4.containsKey(this.clientaddress)&&(m.result().sequenceNum>req.command().sequenceNum);
    //return false;//changed

  }

  @Override
  public synchronized Result getResult() throws InterruptedException {
    // Your code here...
    while (!hasResult()) {
      this.wait();
    }
    return res.result();
    //return null;//changed

  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void handleReply(Reply m, Address sender) {
    // Your code here...
    ///if (Objects.equal(map2.get(m.result().address()),m.result().sequenceNum()))return;
    ///if ((map2.get(m.result().address())>=m.result().sequenceNum()))return;
    if (Objects.equal(map2.get(m.result().address()),m.result().sequenceNum()))return;
    //if (map4.containsKey(sender)&&(m.result().sequenceNum()>req.command().sequenceNum())){
        //req.command().address())req.command().sequenceNum()<=m.result().sequenceNum()){
    res = m.result();
      ///map4.put(sender,m.result().sequenceNum());
    //map2.remove(clientaddress);
    //HashMap<Address,Integer> map2=new HashMap< Address,Integer>();
    map2.put(m.result().address(),res.sequenceNum());
    this.notify();


  }

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void onClientTimer(ClientTimer t) {
    // Your code here...
   //if (Objects.equal(req.command().sequenceNum(),t.command()){

    //if has result,return(not resend timer):
    //if(map2.containsKey(t.command().address())&& Objects.equal(map2.get(t.command().address()),t.command().sequenceNum()))return;
     if(map2.containsKey(t.command().address())&& (map2.get(t.command().address())>=t.command().sequenceNum()))return;
        ///if(hasResult())return;
        //comm = new AMOCommand(t.command().command(),sequenceNum,t.command().address());
        //req=new Request(comm);
        //ClientTimer newt=;
    Request req=new Request(t.command());
    this.send(req,serverAddress);
    this.set(t,CLIENT_RETRY_MILLIS);
      //map2.put(sequenceNum,req);
   // }


  }
}
