package dslabs.paxos;

import dslabs.atmostonce.AMOResult;
import dslabs.framework.Message;
import lombok.Data;

@Data
public final class PaxosReply implements Message {
  // Your code here...
  AMOResult result;
  public PaxosReply(AMOResult res){
    this.result=res;
  }
}
