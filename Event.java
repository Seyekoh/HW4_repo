import java.io.Serializable;

/**
 * Represents an event in the DS. This object is serializable so it can be sent
 * over the network.
 * 
 * @author CS4225
 * @version Spring 2026
 */
public class Event implements Serializable {

        private static final long serialVersionUID = 42252026L;

        private final String sender;
        private final String receiver;
        private final long timestamp;

        public Event(String sender, String receiver, long timestamp) {
                this.sender = sender;
                this.receiver = receiver;
                this.timestamp = timestamp;
        }

        public String getSender() {
                return this.sender;
        }

        public String getReceiver() {
                return this.receiver;
        }

        public long getTimestamp() {
                return this.timestamp;
        }

        @Override
        public String toString() {
                return "Event from " + sender + " to " + receiver + " with timestamp " + timestamp;
        }
}

