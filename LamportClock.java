public class LamportClock {
	private long time;
	private final int nodeId;

	public LamportClock(int nodeId) {
		this.time = 0;
		this.nodeId = nodeId;
	}

	public synchronized long tick() {
		time += nodeId;
		return time;
	}

	public synchronized void update(long receivedTime) {
		time = Math.max(time, receivedTime) + nodeId;
	}

	public synchronized long getTime() {
		return time;
	}
}
