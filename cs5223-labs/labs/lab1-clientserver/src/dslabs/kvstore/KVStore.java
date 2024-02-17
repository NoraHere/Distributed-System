package dslabs.kvstore;

import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.ToString;

import java.util.HashMap;


@ToString
@EqualsAndHashCode
public class KVStore implements Application {

    public interface KVStoreCommand extends Command {}

    public interface SingleKeyCommand extends KVStoreCommand {
        String key();
    }

    @Data
    public static final class Get implements SingleKeyCommand {
        @NonNull private final String key;

        @Override
        public boolean readOnly() {
            return true;
        }
    }

    @Data
    public static final class Put implements SingleKeyCommand {
        @NonNull private final String key, value;
    }

    @Data
    public static final class Append implements SingleKeyCommand {
        @NonNull private final String key, value;
    }

    public interface KVStoreResult extends Result {}

    @Data
    public static final class GetResult implements KVStoreResult {
        @NonNull private final String value;
    }

    @Data
    public static final class KeyNotFound implements KVStoreResult {}

    @Data
    public static final class PutOk implements KVStoreResult {}

    @Data
    public static final class AppendResult implements KVStoreResult {
        @NonNull private final String value;
    }

    // Your code here...
    private final HashMap<String,String> map0= new HashMap<>();

    @Override
    public synchronized KVStoreResult execute(Command command) {
        if (command instanceof Get) {
            Get g = (Get) command;
            // Your code here...
            if (map0.containsKey(g.key())) {
                return new GetResult(map0.get(g.key()));
            }
            return new KeyNotFound();
        }

        if (command instanceof Put) {
            Put p = (Put) command;
            // Your code here...
            map0.put(p.key(), p.value());
            return new PutOk();
        }

        if (command instanceof Append) {
            Append a = (Append) command;
            // Your code here...
            String thevalue;
            synchronized (this) {
                thevalue = map0.getOrDefault(a.key(), "");
                String tvalue = thevalue + a.value();
                map0.put(a.key(), tvalue);
                return new AppendResult(tvalue);
            }
        }
        throw new IllegalArgumentException();
    }
}