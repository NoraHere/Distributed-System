package dslabs.atmostonce;

import dslabs.framework.Command;
import dslabs.framework.Result;
import lombok.Data;

//adding
import dslabs.framework.Address;

@Data
public final class AMOResult implements Result {
  // Your code here...
  Result result;
  int sequenceNum;
  Address address;
  public AMOResult(Result result, int sequenceNum, Address address) {
    this.result = result;
    this.sequenceNum = sequenceNum;
    this.address = address;
  }
}
