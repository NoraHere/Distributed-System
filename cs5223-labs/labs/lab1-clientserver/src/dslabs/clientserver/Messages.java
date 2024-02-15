package dslabs.clientserver;

import dslabs.framework.Message;
import lombok.Data;

//self adding
import dslabs.framework.Result;
import dslabs.framework.Command;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
@Data
class Request implements Message {
  // Your code here...
  private final AMOCommand command;
  //private final int sequenceNum;
}

@Data
class Reply implements Message {
  // Your code here...
  private final AMOResult result;
  //private final int sequenceNum;
}
