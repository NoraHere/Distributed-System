package dslabs.shardmaster;

import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Logger;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;

@ToString
@EqualsAndHashCode
public final class ShardMaster implements Application {
  public static final int INITIAL_CONFIG_NUM = 0;

  private final int numShards;

  // Your code here...
  private static int config_num;
  private HashMap<Integer,Integer> current_config=new HashMap<>();
  private HashMap<Integer,HashMap<Integer,Integer>> config_records= new HashMap<>();//records[config_num]:groupId
  Set<Integer> setGroupId=new HashSet<>();
  private HashMap<Integer,Set<Integer>> config_groups=new HashMap<>();//record[index=config_num]:existing group
  private HashMap<Integer,Set<Address>> group_records=new HashMap<>();//records[groupId]:server address
  private static int group_num=0;

  public ShardMaster(int numShards) {
    this.numShards = numShards;
  }

  public interface ShardMasterCommand extends Command {}

  @Data
  public static final class Join implements ShardMasterCommand {
    private final int groupId;
    private final Set<Address> servers;
  }

  @Data
  public static final class Leave implements ShardMasterCommand {
    private final int groupId;
  }

  @Data
  public static final class Move implements ShardMasterCommand {
    private final int groupId;
    private final int shardNum;
  }

  @Data
  public static final class Query implements ShardMasterCommand {
    private final int configNum;

    @Override
    public boolean readOnly() {
      return true;
    }
  }

  public interface ShardMasterResult extends Result {}

  @Data
  public static final class Ok implements ShardMasterResult {}

  @Data
  public static final class Error implements ShardMasterResult {}

  @Data
  public static final class ShardConfig implements ShardMasterResult {
    private final int configNum;

    // groupId -> <group members, shard numbers>
    private final Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo;
  }

  @Override
  public Result execute(Command command) {
    if (command instanceof Join) {
      Join join = (Join) command;

      // Your code here...
      HashMap<Integer,Integer> backup_config=new HashMap<>(current_config);
      if(config_records.isEmpty()){//initial state
        group_num=1;
        config_num=INITIAL_CONFIG_NUM;
        for(int i=1 ;i<=numShards;i++){
          backup_config.put(i,join.groupId);
        }
      } else{
        if(backup_config.containsValue(join.groupId)){//groupId already exist
          return new Error();
        }
        else {
          group_num++;
          int even = numShards / group_num;//new groupId should exist (even) times
          while (even > 0) {
            HashMap<Integer,Integer> count_group=countHashMap(backup_config);
            int number = findMaxOrMinValue(count_group,true);//groupId
            int num=findKey(backup_config,number);//shard number
            if(Objects.equals(num,0)){
              return new Error();//should not happen
            }
            backup_config.put(num, join.groupId);
            even--;
          }
          checkBalance(backup_config,setGroupId);
          config_num++;
        }
      }
      current_config=backup_config;
      Logger.getLogger("").info("current_config after join: " + current_config);
      config_records.put(config_num, new HashMap<>(current_config)); // Add a copy of current_config
      setGroupId.add(join.groupId);
      config_groups.put(config_num, new HashSet<>(setGroupId)); // Add a copy
      group_records.put(join.groupId, join.servers);
      return new Ok();
    }

    if (command instanceof Leave) {
      Leave leave = (Leave) command;

      // Your code here...
      HashMap<Integer,Integer> backup_config=new HashMap<>(current_config);
      if(!backup_config.containsValue(leave.groupId)){
        return new Error();
      }
      else{
        group_num--;
        HashMap<Integer,Integer> count_group=countHashMap(backup_config);//count occurrence of each groupId
        int even=count_group.get(leave.groupId);
        for(Integer num:backup_config.keySet()){//remove the groupId to null
          if(Objects.equals(backup_config.get(num),leave.groupId)){
            backup_config.put(num,null);
          }
        }
        setGroupId.remove(leave.groupId); // Remove the group ID from the set
        while(even>0){
          int number=findMaxOrMinValue(count_group,false);//groupId
          int num=findKey(backup_config,number);//shard number
          if(Objects.equals(num,0)){
            return new Error();//should not happen
          }
          for(Integer index:backup_config.keySet()){
            if(Objects.equals(backup_config.get(index),null)){//replace null to the smallest occurrence groupId
              backup_config.put(index,backup_config.get(num));
              even--;
              break;
            }
          }
        }
        checkBalance(backup_config,setGroupId);
        config_num++;
        current_config=backup_config;
        Logger.getLogger("").info("current_config after leave: " + current_config);
        config_records.put(config_num,new HashMap<>(current_config));
        config_groups.put(config_num, new HashSet<>(setGroupId));
        return new Ok();
      }
    }

    if (command instanceof Move) {
      Move move = (Move) command;

      // Your code here...
      if(Objects.equals(current_config.get(move.shardNum),move.groupId)||!current_config.containsKey(move.shardNum)
          ||!current_config.containsValue(move.groupId)){
        return new Error();
      }
      else{
        config_num++;
        current_config.put(move.shardNum,move.groupId);
        Logger.getLogger("").info("current_config after move: " + current_config);
        config_records.put(config_num,new HashMap<>(current_config));
        config_groups.put(config_num,new HashSet<>(setGroupId));
        return new Ok();
      }
    }

    if (command instanceof Query) {
      Query query = (Query) command;

      // Your code here...
      if(config_records.isEmpty()){
        return new Error();
      }
      else{
        HashMap<Integer,Integer>config;
        int num;
        Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo=new HashMap<>();// groupId -> <group members, shard numbers>
        Logger.getLogger("").info("config_num: " + config_num);
        if(query.configNum>config_num||Objects.equals(query.configNum,-1)){//get config
          config=config_records.get(config_num);
          num=config_num;
        }
        else{
          config=config_records.get(query.configNum);
          num= query.configNum;;
        }
        //Logger.getLogger("").info("config_groups: " + config_groups);
        //Logger.getLogger("").info("config in shardmaster: " + config);
        //Logger.getLogger("").info("config_num: " + num);
        //System.out.print(num);
        if(Objects.equals(config_records.get(num),null)){
          Logger.getLogger("").info("num: " + num);
          return new Error();
        }
        else{
          for (int groupId : config_groups.get(num)) {
            if (groupInfo.containsKey(groupId)) {
              continue;
            }
            Set<Integer> shardNum = new HashSet<>();
            for (int j = 1; j <= numShards; j++) {
              if (Objects.equals(config.get(j), groupId)) {
                shardNum.add(j);
              }
            }
//        if(shardNum.isEmpty()){//set no leave but be removed groupId's shardNum to be null
//          shardNum=null;
//        }
            Pair<Set<Address>, Set<Integer>> pair=Pair.of(group_records.get(groupId),shardNum);
            groupInfo.put(groupId,pair);
          }

          if(query.configNum>config_num||Objects.equals(query.configNum,-1)){//return
            return new ShardConfig(config_num,groupInfo);
          } else{
            return new ShardConfig(query.configNum,groupInfo);
          }
        }
      }
    }

    throw new IllegalArgumentException();
  }

  public static HashMap<Integer,Integer> countHashMap(HashMap<Integer,Integer> current_config){
    HashMap<Integer,Integer> count_group=new HashMap<>();
    for (Integer groupId : current_config.values()) {
      if (!Objects.equals(groupId,null)) {//skip the null
        count_group.put(groupId, count_group.getOrDefault(groupId, 0) + 1);
      }
    }
    return count_group;
  }
  public static int findMaxOrMinValue(HashMap<Integer,Integer> count_group,Boolean findMax){
    //count current configuration's group number
    //HashMap<Integer,Integer> count_group=countHashMap(current_config);
    int key;
    Integer extremumValue = null;
    Integer extremumKey = null;
    for (Map.Entry<Integer, Integer> entry : count_group.entrySet()) {
      Integer value = entry.getValue();
      if (extremumValue == null || (findMax && value > extremumValue)
          || (!findMax && value < extremumValue)) {
        extremumValue = value;//occurrence times
        extremumKey = entry.getKey();//groupId
      }
    }
    if(extremumValue!=null){
      key=extremumKey;
    }else{
      key=0;//error
    }
    return key;//groupId
  }
  public static int findKey(HashMap<Integer,Integer> config,int sourceValue){
    for (Map.Entry<Integer, Integer> entry : config.entrySet()) {
      Integer value = entry.getValue();
      if (!Objects.equals(value, null) && value.equals(sourceValue)) {
        return entry.getKey();//shard number
      }
    }
    return 0;//error
  }
  public static void checkBalance(HashMap<Integer,Integer> backup_config,Set<Integer> setGroupId){
    //check if need re-balance
    boolean check=true;
    while(check){
      HashMap<Integer,Integer> count_group=countHashMap(backup_config);
      for(Integer id:setGroupId){
        if(!count_group.containsKey(id)){
          count_group.put(id,0);
        }
      }
      int max=count_group.get(findMaxOrMinValue(count_group,true));//max occurrence
      int min=count_group.get(findMaxOrMinValue(count_group,false));//min occurrence
      if(max-min>1){
        backup_config.put(findKey(backup_config,findMaxOrMinValue(count_group,true))
            , findMaxOrMinValue(count_group,false));
      } else{
        check=false;
      }
    }
  }
}
