package dslabs.clientserver;

import dslabs.framework.Timer;
import dslabs.shardmaster.ShardMaster.Query;
import lombok.Data;
//self adding
import dslabs.atmostonce.AMOCommand;

@Data
final class ClientTimer implements Timer {
  static final int CLIENT_RETRY_MILLIS = 100;

  // Your code here...
  private final AMOCommand command;

  ClientTimer(AMOCommand command) {
    this.command = command;
  }
}
