package dslabs.atmostonce;

import dslabs.framework.Command;
import lombok.Data;

//adding
import dslabs.framework.Address;


@Data
public final class AMOCommand implements Command {
  // Your code here...
  private Command command;
  private int sequenceNum;
  private Address address;

  public AMOCommand(Command command, int sequenceNum, Address address) {
    this.command = command;
    this.sequenceNum = sequenceNum;
    this.address = address;
  }

}
