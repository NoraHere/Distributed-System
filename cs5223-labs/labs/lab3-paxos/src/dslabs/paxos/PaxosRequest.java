package dslabs.paxos;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Message;
import lombok.Data;

@Data
public final class PaxosRequest implements Message {
  // Your code here...
  AMOCommand command;
  public PaxosRequest(AMOCommand com){
    this.command=com;
  }
}
