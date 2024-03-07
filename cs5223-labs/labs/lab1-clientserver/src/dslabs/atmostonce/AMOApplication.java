package dslabs.atmostonce;


import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

//adding
import java.util.HashMap;


@EqualsAndHashCode
@ToString
//@RequiredArgsConstructor
public final class AMOApplication<T extends Application> implements Application {
  @Getter @NonNull private final T application;


  // Your code here...
  HashMap<Address,AMOResult> map3=new HashMap<Address,AMOResult>();

  public AMOApplication(T application) {
    this.application = application;
  }



  @Override
  public AMOResult execute(Command command) {
    if (!(command instanceof AMOCommand)) {
      throw new IllegalArgumentException();
    }

    AMOCommand amoCommand = (AMOCommand) command;

    // Your code here...
    if (!alreadyExecuted(amoCommand)){
      //Request req= new Request(amoCommand);
      //sequenceNum=amoCommand.sequenceNum()+1;
      ///res=new AMOResult(application.execute(amoCommand.command()),amoCommand.sequenceNum()+1,amoCommand.address());
      AMOResult res=new AMOResult(application.execute(amoCommand.command()),amoCommand.sequenceNum(),amoCommand.address());
      map3.put(amoCommand.address(),res);
      //res.sequenceNum= ;
      //res.address=;
      return res;
    }
    //res=new AMOResult(map3.get(amoCommand.address()),amoCommand.sequenceNum(),amoCommand.address());
    return map3.get(amoCommand.address());
  }

  public Result executeReadOnly(Command command) {
    if (!command.readOnly()) {
      throw new IllegalArgumentException();
    }

    if (command instanceof AMOCommand) {
      return execute(command);
    }

    return application.execute(command);
  }

  public boolean alreadyExecuted(AMOCommand amoCommand) {
    // Your code here...
    ///if(map3.containsKey(amoCommand.address())&&(map3.get(amoCommand.address())>amoCommand.sequenceNum())) return true;//old request
    if(map3.containsKey(amoCommand.address())&& map3.get(amoCommand.address()).sequenceNum()>=amoCommand.sequenceNum()) return true;//old request

    return false;
  }
}
