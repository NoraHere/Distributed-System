package dslabs.primarybackup;

import dslabs.framework.Address;
import java.io.Serializable;
import lombok.Data;

@Data
class View implements Serializable {
  private final int viewNum;
  private final Address primary, backup;

  //self adding
  public View(int viewnum,Address primary, Address backup){
    this.backup=backup;
    this.viewNum=viewnum;
    this.primary=primary;
  }

  public int getViewNum() {
    return viewNum;
  }

  public Address getBackup() {
    return backup;
  }

  public Address getPrimary() {
    return primary;
  }
}
