package cs490_assignment3;

public interface CasualReliableBroadcast {
	public void init(Process currentProcess, BroadcastReceiver br);
	public void addMember(Process member);
	public void removeMember(Process member);
	public void crbroadcast(Message m);
}
