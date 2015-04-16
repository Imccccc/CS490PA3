package cs490_assignment3;

public class MessageImp implements Message, Comparable<Message>{

	String contents;
	VectorClock vc;
	
	public MessageImp(String s){
		String[] info = s.split("\\|");
		this.contents = info[0];
		this.vc = new VectorClock(info[1]);
	}
	
	public MessageImp(String s, VectorClock vc){
		this.contents = new String(s);
		this.vc = vc;
	}
	
	@Override
	public VectorClock getMessageVC() {
		return this.vc;
	}

	@Override
	public void setMessageVC(VectorClock vc) {
		this.vc = vc;
	}

	@Override
	public String getMessageContents() {
		return contents;
	}

	@Override
	public void setMessageContents(String contents) {
		this.contents = new String(contents);
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Message))
			return false;	
		if (obj == this)
			return true;

		return this.contents.equals(((Message) obj).getMessageContents()) && this.vc.compareTo(((Message) obj).getMessageVC())==0;
	}
 
	@Override
	public int hashCode(){
		return this.contents.length();//for simplicity reason
	}
	
    @Override
    public int compareTo(Message o) {
        return this.vc.compareTo(o.getMessageVC());
    }
    
    @Override
    public String toString(){
		return this.contents+"|"+this.vc.toString();
    }    
}
