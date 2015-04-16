package cs490_assignment3;

public interface Message{
	public VectorClock getMessageVC();
	public void setMessageVC(VectorClock VC);
	public String getMessageContents();
	public void setMessageContents(String contents);
}
