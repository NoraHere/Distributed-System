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
  private Address address;//clientAddress

  public AMOCommand(Command command, int sequenceNum, Address address) {
    this.command = command;
    this.sequenceNum = sequenceNum;
    this.address = address;
  }
  public static Command getCommand(AMOCommand com){return com.command;}
  public static int getSequenceNum(AMOCommand com){return com.sequenceNum;}
  public static Address getAddress(AMOCommand com){return com.address;}
}
