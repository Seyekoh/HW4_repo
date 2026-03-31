public class LamportClock {
	private long time;
	private final int increment;

	public LamportClock(int increment) {
		this.time = 0;
		this.increment = increment;
	}

	public synchronized long tick() {
		time += increment;
		return time;
	}

	public synchronized void update(long receivedTime) {
		time = Math.max(time, receivedTime) + increment;
	}

	public synchronized long getTime() {
		return time;
	}
}
