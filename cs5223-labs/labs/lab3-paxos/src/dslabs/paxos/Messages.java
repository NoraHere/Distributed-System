package dslabs.paxos;

// Your code here...
import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Message;
import dslabs.framework.Address;
import java.util.ArrayList;
import dslabs.paxos.PaxosServer.pvalue;

class Heartbeat implements Message{

}
class Proposal implements Message {
  int slot_num;
  AMOCommand com;
  public Proposal(int slot_num,AMOCommand com){
    this.slot_num=slot_num;
    this.com=com;
  }

}
class Decision implements Message{
  int slot;
  AMOCommand com;
  public Decision(int slot,AMOCommand com){
    this.slot=slot;
    this.com=com;
  }
}

class Phase1a implements Message{
  double ballot_num;
  public Phase1a(double num){
    this.ballot_num=num;
  }
}

class Phase1b implements Message{
  double ballot_num;
  ArrayList<pvalue> accepted;
  public Phase1b(double num,ArrayList<pvalue> accepted){
    this.ballot_num=num;
    this.accepted=accepted;
  }

}
class Phase2a implements Message{
  double ballot_num;
  int slot_num;
  AMOCommand com;
  public Phase2a(double ballot_num,int slot_num,AMOCommand com){
    this.ballot_num=ballot_num;
    this.slot_num=slot_num;
    this.com=com;
  }
}
class Phase2b implements Message{
  double ballot_num;
  int slot_num;
  public Phase2b(double num,int slot){
    this.ballot_num=num;
    this.slot_num=slot;
  }
}