package dslabs.paxos;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Timer;
import lombok.Data;

@Data
final class ClientTimer implements Timer {
  static final int CLIENT_RETRY_MILLIS = 100;

  // Your code here...
  AMOCommand command;
  public ClientTimer(AMOCommand com){
    this.command=com;
  }
}

// Your code here...
class HeartBeatTimer implements Timer{
  static final int RETRY_MILLIS=50;
}
class CheckActive implements Timer{
  static final int RETRY_MILLIS=200;
}
class Phase1aTimer implements Timer{
  static final int RETRY_MILLIS=50;
  double num;
  Phase1aTimer(double num){
    this.num=num;
  }
}
//class ProposalTimer implements Timer{
//  static final int RETRY_MILLIS=200;
//  int slot_num;
//  AMOCommand com;
//  public ProposalTimer(int slot_num,AMOCommand com){
//    this.slot_num=slot_num;
//    this.com=com;
//  }
//}

class Phase2aTimer implements Timer{
  static final int RETRY_MILLIS=50;
  double ballot_num;
  int slot_num;
  AMOCommand com;
  Phase2aTimer(double ballot_num,int slot_num,AMOCommand com){
    this.ballot_num=ballot_num;
    this.slot_num=slot_num;
    this.com=com;
  }
}