package dslabs.primarybackup;

import dslabs.framework.Message;
import lombok.Data;

/* -----------------------------------------------------------------------------------------------
 *  ViewServer Messages
 * ---------------------------------------------------------------------------------------------*/
@Data
class Ping implements Message {
  private final int viewNum;

  //self adding
  public Ping(int viewNum){
    this.viewNum=viewNum;
  }
  public int getViewNum(){
    return this.viewNum;
  }
}

@Data
class GetView implements Message {}

@Data
class ViewReply implements Message {
  private final View view;
  //self-adding
  public ViewReply(View view){
    this.view=view;
  }
}

/* -----------------------------------------------------------------------------------------------
 *  Primary-Backup Messages
 * ---------------------------------------------------------------------------------------------*/
@Data
class Request implements Message {
  // Your code here...
}

@Data
class Reply implements Message {
  // Your code here...
}

// Your code here...
