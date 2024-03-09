package dslabs.primarybackup;

import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;

import dslabs.framework.Message;
import java.util.ArrayList;
import lombok.Data;

/* -----------------------------------------------------------------------------------------------
 *  ViewServer Messages
 * ---------------------------------------------------------------------------------------------*/
@Data
class Ping implements Message {
  private final int viewNum;

  //self adding
  public Ping(int viewNum){
    this.viewNum=viewNum;
  }
  public int getViewNum(){
    return this.viewNum;
  }
}

@Data
class GetView implements Message {}

@Data
class ViewReply implements Message {
  private final View view;
  //self-adding
  public ViewReply(View view){
    this.view=view;
  }
  public static View getView(ViewReply viewReply)  {return viewReply.view;}
}

/* -----------------------------------------------------------------------------------------------
 *  Primary-Backup Messages
 * ---------------------------------------------------------------------------------------------*/
@Data
class Request implements Message {
  // Your code here...
  private AMOCommand command;
  public Request(AMOCommand command){this.command=command;}
  public static AMOCommand getCommand(Request req){return req.command;}
}

@Data
class Reply implements Message {
  // Your code here...
  private AMOResult result;
  private Boolean resultTrue;
  public Reply(AMOResult result,Boolean resultTrue){
    this.result=result;
    this.resultTrue=resultTrue;
  }
  public static AMOResult getResult(Reply rep) {return rep.result;}
  public static Boolean getTrue(Reply rep){return rep.resultTrue;}
}

// Your code here...
class TransferState implements Message{//transfer the complete application state
  //private ArrayList<Object> operationList;
  private ArrayList<ArrayList<Object>> operationList;
  private int transferNum;//sequence number record transfer
  //public  TransferState(ArrayList<Object> operationList,int transferNum){
  public  TransferState(ArrayList<ArrayList<Object>> operationList,int transferNum){
    this.operationList=operationList;
    this.transferNum=transferNum;
  }
  //public static ArrayList<Object> getOperationList(TransferState ts){return ts.operationList;}
  public static ArrayList<ArrayList<Object>> getOperationList(TransferState ts){return ts.operationList;}
  public static int getTransferNum(TransferState ts){return ts.transferNum;}
}

class TransferReply implements Message{//backup reply transfer status
  //private ArrayList<Object> operationList;
  private ArrayList<ArrayList<Object>> operationList;
  private int transferNum;
  //public TransferReply(ArrayList<Object> operationList, int transferNum){
  public TransferReply(ArrayList<ArrayList<Object>> operationList, int transferNum){
    this.transferNum=transferNum;
    this.operationList=operationList;
  }
  public static int getTransferNum(TransferReply tr){return tr.transferNum;}
  //public static ArrayList<Object> getOperationList(TransferReply tr){return tr.operationList;}
  public static ArrayList<ArrayList<Object>> getOperationList(TransferReply tr){return tr.operationList;}
}

class PrimaryRequest implements Message{//message primary send to backup
  private Request request;
  //private Address clientAdd;
  private int primarySeqNum;//sequenceNum
  public PrimaryRequest(Request request,int primarySeqNum){
    this.request=request;
    this.primarySeqNum=primarySeqNum;
  }
  public static Request getRequest(PrimaryRequest pr){return pr.request;}
  public static int getPrimarySeqNum(PrimaryRequest pr){return pr.primarySeqNum;}
}

class BackupReply implements Message{//message backup send to primary
  private String message;
  private Request request;
  private int priSeqNum;//sequence number between primary and backup
  public BackupReply(String message,Request request,int priSeqNum){
    this.message=message;
    this.request=request;
    this.priSeqNum=priSeqNum;
  }
  public static String getBackupMessage(BackupReply br){return br.message;}
  public static Request getRequest(BackupReply br){return br.request;}
  public static int getPriSeqNum(BackupReply br){return br.priSeqNum;}
}