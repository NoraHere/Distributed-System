package dslabs.clientserver;


import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Node;

import lombok.EqualsAndHashCode;
import lombok.ToString;


//self adding
import static dslabs.clientserver.ClientTimer.CLIENT_RETRY_MILLIS;
import java.util.HashMap;
import dslabs.framework.Result;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.atmostonce.AMOApplication;
import dslabs.clientserver.SimpleClient;


/**
 * Simple server that receives requests and returns responses.
 *
 * <p>See the documentation of {@link Node} for important implementation notes.
 */
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class SimpleServer extends Node {
  // Your code here...

  private final Application app;
  //private final KVStore kvstore=new KVStore();
  private AMOResult res;
  private AMOApplication myamo;
  ///HashMap<Address,Integer> map1=new HashMap<Address,Integer>();
  private SimpleClient client;

  //client's address and request stores in map1 by server

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public SimpleServer(Address address, Application app) {
    super(address);

    // Your code here...
    this.app=app;
    myamo=new AMOApplication<>(this.app);

  }

  @Override
  public void init() {
    // No initialization necessary
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void handleRequest(Request m, Address sender) {
    // Your code here...
    //int tepo = m.command().sequenceNum();//sequenceNum temporary
   // if(map1.containsKey(m)
    //tepo++;
    //client=new SimpleClient(sender,m.command().address());
    //if (client.hasResult())return;
    ///if(map1.containsKey(m.command().address()&&)m.command().sequenceNum())
    res=myamo.execute(m.command());
    //res=new AMOResult(app.execute(m.command().command()),m.command().sequenceNum(),sender);
    //res.result( myamo.execute(m.command()));//command is request sent
    //res.sequenceNum(m.command().sequenceNum());
    //res.address(m.command().address());
    //if (res==null)return;
    this.send(new Reply(res), sender);//result is not null
    ///map1.put(m.command().address(),m.command().sequenceNum());//  }
  }
}
