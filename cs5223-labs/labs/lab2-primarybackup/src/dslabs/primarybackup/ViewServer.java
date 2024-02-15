package dslabs.primarybackup;

import static dslabs.primarybackup.PingCheckTimer.PING_CHECK_MILLIS;

import dslabs.framework.Address;
import dslabs.framework.Node;
import java.util.Objects;
import lombok.EqualsAndHashCode;
import lombok.ToString;

//self adding
import java.util.HashMap;
import java.util.Random;
import dslabs.primarybackup.View;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class ViewServer extends Node {
  static final int STARTUP_VIEWNUM = 0;
  private static final int INITIAL_VIEWNUM = 1;

  // Your code here...
  private View currentview;
  private int currentviewNum;
  private Address cprimaryAdd;//current primary address
  private Address cbackupAdd;//current backup address
  private HashMap<Address, Boolean> serveralive=new HashMap<>();//record alive server
  private int serViewNum;//server most recent viewNum
  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public ViewServer(Address address) {
    super(address);
  }

  @Override
  public void init() {
    set(new PingCheckTimer(), PING_CHECK_MILLIS);
    // Your code here...
    currentviewNum=STARTUP_VIEWNUM;
    View iniview=new View(currentviewNum,null,null);//initial any primary server and a backup
    currentview=iniview;
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void handlePing(Ping m, Address sender) {
    // Your code here...
    serViewNum =m.getViewNum();
    serveralive.put(sender,true);//record this sender is alive
    this.send(new ViewReply(currentview),sender);
  }

  private void handleGetView(GetView m, Address sender) {
    // Your code here...
    this.send(new ViewReply(currentview),sender);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void onPingCheckTimer(PingCheckTimer t) {
    // Your code here...
    //decide whether server is alive/dead
    if (Objects.equals(currentviewNum,0)){//Go to first state
      cprimaryAdd=selectServer(serveralive);
      serveralive.remove(cprimaryAdd);
      cbackupAdd=selectServer(serveralive);
      currentviewNum=INITIAL_VIEWNUM;
      currentview=new View(currentviewNum,cprimaryAdd,cbackupAdd);
      serveralive=new HashMap<>();//resetting status of whether receive a ping
    }
    if (!Objects.equals(cbackupAdd,null)) {
      if (!serveralive.containsKey(cprimaryAdd)) {//if don't receive ping from primary
        currentviewNum += 1;
        cprimaryAdd = cbackupAdd;//make backup be new primary
        serveralive.remove(cprimaryAdd);
        cbackupAdd = selectServer(serveralive);
        currentview = new View(currentviewNum, cprimaryAdd, cbackupAdd);
      }

      if (!serveralive.containsKey(cbackupAdd)) {//if don't receive ping from backup
        currentviewNum += 1;
        serveralive.remove(cprimaryAdd);
        cbackupAdd = selectServer(serveralive);//select new backup
        currentview = new View(currentviewNum, cprimaryAdd, cbackupAdd);
      }
    }else{//has idle server
      //serveralive.containsKey()
    }

    //send(Ping,)
    this.set(t, PING_CHECK_MILLIS);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Utils
   * ---------------------------------------------------------------------------------------------*/
  // Your code here...
  private Address selectServer(HashMap<Address,Boolean> m){//select random server
    Address[] keys = m.keySet().toArray(new Address[0]);
    // Generate a random index within the range of the number of keys
    Random random = new Random();
    int randomIndex = random.nextInt(keys.length);
    Address randomKey = keys[randomIndex];//this is random selected primary
    return randomKey;
  }


}
