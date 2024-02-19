package dslabs.primarybackup;

import static dslabs.primarybackup.PingCheckTimer.PING_CHECK_MILLIS;

import dslabs.framework.Address;
import dslabs.framework.Node;
import java.util.ArrayList;
import java.util.Objects;
import lombok.EqualsAndHashCode;
import lombok.ToString;

//self adding
import java.util.HashMap;
import java.util.Random;
import java.util.LinkedHashSet;
import java.util.HashSet;
import java.util.Iterator;
import dslabs.primarybackup.View;
import org.checkerframework.checker.units.qual.A;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class ViewServer extends Node {

  static final int STARTUP_VIEWNUM = 0;
  static final int INITIAL_VIEWNUM = 1;

  // Your code here...
  private View currentview;
  private int currentViewNum;
  private Address cprimaryAdd;//current primary address
  private Address cbackupAdd;//current backup address
  private HashMap<Address, Integer> serveralive_f = new HashMap<>();//record alive server and view number it knows
  private HashMap<Address, Integer> serveralive_s= new HashMap<>();//second consecutive time alive server
  private int serViewNum;// most recent viewNum server knows

  private int priViewNum;//primary acknowledge view number

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
    //moving to the above
    currentViewNum = STARTUP_VIEWNUM;
    cprimaryAdd=null;
    cbackupAdd=null;
    currentview = new View(currentViewNum, cprimaryAdd, cbackupAdd);

  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void handlePing(Ping m, Address sender) {
    // Your code here...
    serViewNum = m.getViewNum();
    serveralive_f.put(sender, serViewNum);//record this sender is alive, and viewNum it knows

    //initialize after first ping
    if (Objects.equals(cprimaryAdd, null) && Objects.equals(currentViewNum, STARTUP_VIEWNUM)) {
      currentViewNum = INITIAL_VIEWNUM;
      cprimaryAdd = sender;
      currentview = new View(currentViewNum, cprimaryAdd, cbackupAdd);
    }else { //later stage

      if(Objects.equals(sender,cprimaryAdd)){
        priViewNum=serViewNum;
      }

      changeView();
    }
    this.send(new ViewReply(currentview), sender);
  }

  private void handleGetView(GetView m, Address sender) {
    // Your code here...
    //System.out.println(serveralive);
    this.send(new ViewReply(currentview), sender);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void onPingCheckTimer(PingCheckTimer t) {
    // Your code here...

  //garbage collect server status over two consecutive time
    serveralive_s.clear();
    serveralive_s.putAll(serveralive_f);
    serveralive_f.clear();

    //check if primary/backup dead and change current view
    changeView();

    //check within two consecutive PingCheckTimer
    this.set(t, PING_CHECK_MILLIS);

  }

  /* -----------------------------------------------------------------------------------------------
   *  Utils
   * ---------------------------------------------------------------------------------------------*/
  // Your code here...
  private Address selectServer(HashMap<Address,Integer> m1,HashMap<Address,Integer> m2,Address cprimaryAdd){//select random server
    //Address[] keys = m.keySet().toArray(new Address[0]);
    LinkedHashSet<Address> keys = new LinkedHashSet<>(m1.keySet());
    keys.addAll(m2.keySet());//combine two consecutive time alive server without duplicate
    ArrayList<Address> newkey=new ArrayList<>(keys);
    newkey.remove(cprimaryAdd);
    // Generate a random index within the range of the number of keys
    if (newkey.isEmpty()) return null;//no server to select
    else if (newkey.size()==1) {//only one server available
      return newkey.get(0);
    }
    Random random = new Random();
    int randomIndex = random.nextInt(newkey.size());
    return newkey.get(randomIndex);//this is random selected primary
  }

  private void changeView(){
    //primary acknowledges correct current view number
    if (Objects.equals(priViewNum, currentViewNum)) {

      //view service proceed to a new view
      if (!Objects.equals(cbackupAdd, null)){//backup exist
        if ((!serveralive_f.containsKey(cprimaryAdd)) && (!serveralive_s.containsKey(cprimaryAdd))) {
          //don't receive ping from primary
          this.currentViewNum += 1;
          this.cprimaryAdd = cbackupAdd;//make backup be new primary
          this.cbackupAdd = selectServer(serveralive_f, serveralive_s, cprimaryAdd);
          this.currentview = new View(currentViewNum, cprimaryAdd, cbackupAdd);
        }
        else if (!serveralive_f.containsKey(cbackupAdd) && !serveralive_s.containsKey(cbackupAdd)) {
          //don't receive ping from backup
          this.currentViewNum += 1;
          this.cbackupAdd = selectServer(serveralive_f, serveralive_s, cprimaryAdd);//select new backup
          this.currentview = new View(currentViewNum, cprimaryAdd, cbackupAdd);
        }
      } else {//no backup
        this.cbackupAdd = selectServer(serveralive_f, serveralive_s, cprimaryAdd);//select other than cprimary add
        if (!Objects.equals(cbackupAdd, null)) {//has idle server
          this.currentViewNum += 1;
          this.currentview = new View(currentViewNum, cprimaryAdd, cbackupAdd);
        }
      }

    }
  }

}
