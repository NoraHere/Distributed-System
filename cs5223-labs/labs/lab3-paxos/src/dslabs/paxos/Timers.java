package dslabs.paxos;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Timer;
import lombok.Data;

@Data
final class ClientTimer implements Timer {
  static final int CLIENT_RETRY_MILLIS = 100;

  // Your code here...
  private final AMOCommand command;
}

// Your code here...
@Data
class HeartBeatTimer implements Timer{
  static final int RETRY_MILLIS=20;
}
@Data
class CheckActive implements Timer{
  static final int RETRY_MILLIS=100;
}
@Data
class Phase1aTimer implements Timer{
  static final int RETRY_MILLIS=20;
  private final double num;
}
@Data
class Phase2aTimer implements Timer{
  static final int RETRY_MILLIS=20;
  private final double ballot_num;
  private final int slot_num;
  private final AMOCommand com;
}