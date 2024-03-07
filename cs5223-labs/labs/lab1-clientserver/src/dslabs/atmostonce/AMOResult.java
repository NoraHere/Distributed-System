package dslabs.atmostonce;

import dslabs.framework.Result;
import lombok.Data;

//adding
import dslabs.framework.Address;

@Data
public final class AMOResult implements Result {
  // Your code here...
  private Result result;
  private int sequenceNum;
  private Address address;
  public AMOResult(Result result, int sequenceNum, Address address) {
    this.result = result;
    this.sequenceNum = sequenceNum;
    this.address = address;
  }
  public static Result getResult(AMOResult m){return m.result;}
  public static int getSequenceNum(AMOResult m){return m.sequenceNum;}
  public static Address getAddress(AMOResult m){return m.address;}
}
