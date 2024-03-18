package dslabs.primarybackup;

import dslabs.atmostonce.AMOCommand;

import dslabs.framework.Command;
import dslabs.framework.Timer;
import java.util.ArrayList;
import lombok.Data;

//self-adding
import dslabs.framework.Address;

@Data
final class PingCheckTimer implements Timer {
  static final int PING_CHECK_MILLIS = 100;
}

@Data
final class PingTimer implements Timer {
  static final int PING_MILLIS = 25;
}

@Data
final class ClientTimer implements Timer {
  static final int CLIENT_RETRY_MILLIS = 100;

  // Your code here...
  private final AMOCommand command;

  ClientTimer(AMOCommand command) {
    this.command = command;
  }
  public static AMOCommand getCommand(ClientTimer t){return t.command;}
}

final class RetryTimer implements Timer{//retry to send GetView()
  static final int RETRY_MILLIS=100;
  private final Address pastPrimary;

  RetryTimer(Address pastPrimary) {
    this.pastPrimary = pastPrimary;
  }
  public static Address getPastPrimary(RetryTimer t){return t.pastPrimary;}
}

// Your code here...
final class PrimaryRequestTimer implements Timer{
  static final int CHECK_MILLIS=100;
  private final Request request;
  private final Address sender;
  PrimaryRequestTimer(Request request,Address sender){
    this.request=request;
    this.sender=sender;}
  public static Request getRequest(PrimaryRequestTimer t){return t.request;}
  public static Address getSenderAdd(PrimaryRequestTimer t){return t.sender;}
}

final class TransferCheckTimer implements Timer{
  static final int CHECK_MILLIS=100;
  //private final ArrayList<Object> operationList;
  private final ArrayList<ArrayList<Object>> operationList;
  private final Address sender;
  //TransferCheckTimer(ArrayList<Object> operationList){this.operationList=operationList;}
  TransferCheckTimer(ArrayList<ArrayList<Object>> operationList,Address sender){
    this.operationList=operationList;
    this.sender=sender;}
  //public static ArrayList<Object> getOperationList(TransferCheckTimer t){return t.operationList;}
  public static ArrayList<ArrayList<Object>> getOperationList(TransferCheckTimer t){return t.operationList;}
  public static Address getSenderAdd(TransferCheckTimer t){return t.sender;}
}