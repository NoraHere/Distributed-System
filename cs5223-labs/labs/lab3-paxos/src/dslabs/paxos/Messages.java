package dslabs.paxos;

// Your code here...
import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Message;
import dslabs.framework.Address;
import java.util.ArrayList;
import java.util.HashMap;
import dslabs.paxos.PaxosServer.pvalue;
import lombok.Data;
@Data
class Heartbeat implements Message{
  private final int cleared;
  private final double ballot;
}
@Data
class HeartbeatReply implements Message{
  private final int slot_out;
}
@Data
class Decision implements Message{
  private final int slot;
  private final AMOCommand com;
}
@Data
class Phase1a implements Message{
  private final double ballot_num;
}
@Data
class Phase1b implements Message{
  private final double ballot_num;//acceptor ballot number
  private final HashMap<Integer,pvalue> accepted;//{slot_num:pvalues<b,s,c>,...}
  private final HashMap<Integer,AMOCommand> decisions;//save decisions
}
@Data
class Phase2a implements Message{
  private final double ballot_num;
  private final int slot_num;
  private final AMOCommand com;
}
@Data
class Phase2b implements Message{
  private final double ballot_num;
  private final int slot_num;
}

//@Data
//class pvalue{
//  private final double ballot_num;
//  private final int slot_num;
//  private final AMOCommand com;
//}