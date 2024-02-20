package dslabs.primarybackup;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Node;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Objects;
import lombok.EqualsAndHashCode;
import lombok.ToString;

//self-adding
import java.util.ArrayList;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class PBServer extends Node {
  private final Address viewServer;

  // Your code here...
  private Address address;
  private Application app;
  private AMOApplication amoapp;
  private AMOResult res;
  private int viewNum=ViewServer.STARTUP_VIEWNUM;
  private View currentView;
  private Address currentPrimary;
  private Address currentBackup;
  private Request request;
  private Address clientAdd;//client address
  int priSeqNum=0;//primary sequence number
  int transferNum=0;//primary send transfer state sequence number

  private AMOCommand command;

  //private ArrayList<Object> operationList= new ArrayList<>();//record all operations
  private ArrayList<ArrayList<Object>> operationList = new ArrayList<>();
  private HashMap<Request,Integer> temOperationList=new HashMap<>();//record all unconfirmed operations
  private HashMap<Request,Integer> mapBackupReply=new HashMap<>();//record received backup reply
  //private  HashMap<ArrayList<Object>,Integer> transferList=new HashMap<>();//record transferred operationList
  private  HashMap<ArrayList<ArrayList<Object>>,Integer> transferList=new HashMap<>();
  private HashMap<Address,Reply> replyToClient=new HashMap<>();
  private HashMap<Address,BackupReply> replyToPrimary=new HashMap<>();
  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  PBServer(Address address, Address viewServer, Application app) {
    super(address);
    this.viewServer = viewServer;

    // Your code here...
    this.address=address;
    this.app=app;
    amoapp=new AMOApplication<>(this.app);
  }

  @Override
  public void init() {
    // Your code here...
    send(new Ping(viewNum),viewServer);
    set(new PingTimer(),PingTimer.PING_MILLIS);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void handleRequest(Request m, Address sender) {
    // Your code here...
    //from client
    //don't respond to clients if isn't the active primary
    if(!Objects.equals(this.address,currentPrimary)) return;
    //is primary
    request=m;
    command=Request.getCommand(m);
    int seqNum=AMOCommand.getSequenceNum(command);
    clientAdd=sender;

    //may have handled
    if(replyToClient.containsKey(sender)) {
      Reply mayReply = replyToClient.get(sender);
      AMOResult mayResult = Reply.getResult(mayReply);
      int maySeqNum = AMOResult.getSequenceNum(mayResult);
      if (Objects.equals(maySeqNum, seqNum)) {
        send(mayReply, sender);
        return;
      }
    }

    //forward the request to backup
    if (Objects.equals(currentBackup,null)) {//no backup
      operationList.add(new ArrayList<>(Arrays.asList(request,clientAdd)));
      res=amoapp.execute(command);
      Reply reply=new Reply(res,true);
      send(reply,sender);
      replyToClient.put(clientAdd,reply);//record the reply
    }
    else{
      priSeqNum++;//between primary and backup
      temOperationList.put(request,priSeqNum);//record the unconfirmed request in primary
      send(new PrimaryRequest(request,priSeqNum), currentBackup);//timer resend if no reply from backup
      set(new PrimaryRequestTimer(request),PrimaryRequestTimer.CHECK_MILLIS);
      //get reply from backup: ok or reject
    }

  }

  private void handleViewReply(ViewReply m, Address sender) {
    // Your code here...
   currentView=ViewReply.getView(m);
   currentPrimary=currentView.getPrimary();
   viewNum=currentView.getViewNum();
   Address pastBackupAdd=currentBackup;
   currentBackup = currentView.getBackup();

   if(Objects.equals(address,currentPrimary)){//primary
     if(!Objects.equals(currentBackup,null)) {//has backup
       if (!Objects.equals(currentBackup, pastBackupAdd)) {//has new backup => transfer state
         //make current Backupserver operationList empty
         //PBServer(currentBackup,viewServer,app).operationList.empty();
         transferNum++;
         transferList.put(operationList, transferNum);
         send(new TransferState(operationList, transferNum), currentBackup);//timer resend
         set(new TransferCheckTimer(operationList), TransferCheckTimer.CHECK_MILLIS);
       }
     }
   }
   //this.notify();;
  }

  // Your code here...
  private void handlePrimaryRequest(PrimaryRequest pr,Address sender){
    //backup
    if(!Objects.equals(this.address,currentBackup))return;
    request=PrimaryRequest.getRequest(pr);//from PrimaryRequest
    int priSeqNum=PrimaryRequest.getPrimarySeqNum(pr);//from PrimaryRequest

    //may have handled
    if(replyToPrimary.containsKey(sender)) {
      BackupReply mayBackupReply = replyToPrimary.get(sender);
      int mayPriNum = BackupReply.getPriSeqNum(mayBackupReply);
      if (Objects.equals(mayPriNum, priSeqNum)) {
        send(mayBackupReply, sender);
        return;
      }
    }

    AMOCommand command=Request.getCommand(request);
    clientAdd=AMOCommand.getAddress(command);
    String message;
    if(Objects.equals(sender,currentPrimary)){//sender is primary
      message="RIGHT";
      operationList.add(new ArrayList<>(Arrays.asList(request,clientAdd)));
      res=amoapp.execute(command);
    }
    else{
      message="ERROR";
    }
    BackupReply backupReply=new BackupReply(message,request,priSeqNum);
    send(backupReply,sender);
    replyToPrimary.put(sender,backupReply);
  }

  private void handleBackupReply(BackupReply br,Address sender){
    //primary
    if(!Objects.equals(this.address,currentPrimary))return;//???
    //what if the old primary realize it's not primary after send PrimaryRequest to backup
    //this way it can never reply error to client
    Boolean resultTrue=true;
    String message=BackupReply.getBackupMessage(br);//get from BackupReply
    Request request=BackupReply.getRequest(br);//get from BackupReply
    int priSeqNumFromBackup=BackupReply.getPriSeqNum(br);//get from BackupReply

    //if(Objects.equals(mapBackupReply.get(request),temOperationList.get(request)))return;
    //backup send past reply
    if(!temOperationList.containsKey(request))return;

    AMOCommand command=Request.getCommand(request);
    Address clientAdd=AMOCommand.getAddress(command);

    if(Objects.equals(message,"RIGHT")){
      res=amoapp.execute(command);
      operationList.add(new ArrayList<>(Arrays.asList(request,clientAdd)));
    }
    else if(Objects.equals(message,"ERROR")){
      resultTrue=false;
    }
    Reply reply=new Reply(res,resultTrue);
    send(reply,clientAdd);
    replyToClient.put(clientAdd,reply);//record the reply
    mapBackupReply.put(request,priSeqNumFromBackup);
    //temOperationList.remove(request);//received BackupReply
    //this.notify();;
  }
  private void handleTransferReply(TransferReply tr,Address sender){
    //primary
    if(!Objects.equals(this.address,currentPrimary))return;
    int transferNumFromBackup=TransferReply.getTransferNum(tr);
    ArrayList<ArrayList<Object>> operationListFromBackup=TransferReply.getOperationList(tr);

    //already successfully transfer
    if(Objects.equals(transferList.get(operationListFromBackup),transferNumFromBackup)) {
      //transferList.remove(operationList);
      return;
    }
    else {
      send(new TransferState(operationList,transferList.get(operationList)),currentBackup);
      set(new TransferCheckTimer(operationList),TransferCheckTimer.CHECK_MILLIS);
    }
    //this.notify();

  }

  private void handleTransferState(TransferState ts,Address sender){
    //backup
    if(!Objects.equals(this.address,currentBackup))return;
    operationList=TransferState.getOperationList(ts);//get from transferState
    int transferNum=TransferState.getTransferNum(ts);//get from transferState
    //execute all request getting from transfer
    for(int i=0;i<operationList.size();i++){
      Request request= (Request) operationList.get(i).get(0);//Type Object=>Request may has error!!
      AMOCommand cm=Request.getCommand(request);
      res=amoapp.execute(cm);
    }
    send(new TransferReply(operationList,transferNum),sender);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void onPingTimer(PingTimer t) {
    // Your code here...
    //periodically Ping the viewServer
    send(new Ping(viewNum),viewServer);
    set(new PingTimer(),PingTimer.PING_MILLIS);
  }

  // Your code here...
  private void onPrimaryRequestTimer(PrimaryRequestTimer t){
    //if receive BackupReply return;
    //if()return;
    Request request=PrimaryRequestTimer.getRequest(t);
    if(mapBackupReply.containsKey(request))return;//received reply from backup
    send(new PrimaryRequest(request,temOperationList.get(request)), currentBackup);
    set(new PrimaryRequestTimer(request),PrimaryRequestTimer.CHECK_MILLIS);
  }

  private void onTransferCheckTimer(TransferCheckTimer t){
    //if already transfer all operation, return;
    ArrayList<ArrayList<Object>> operationList=TransferCheckTimer.getOperationList(t);
    if(!transferList.containsKey(operationList))return;//received TransferReply
    if (!Objects.equals(currentBackup,null)) {
      send(new TransferState(operationList, transferList.get(operationList)), currentBackup);
      set(new TransferCheckTimer(operationList), TransferCheckTimer.CHECK_MILLIS);
    }
  }

  /* -----------------------------------------------------------------------------------------------
   *  Utils
   * ---------------------------------------------------------------------------------------------*/
  // Your code here...
  public ArrayList<ArrayList<Object>> getOperationList(){
    return this.operationList;
  }
}
