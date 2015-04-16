package cs490_assignment3;

public class Process{
	private String IP;
	private int port;
	private String ID;
	
	public Process(String IP, int port, String ID){
		this.IP = IP;
		this.port = port;
		this.ID = ID;
	}
	
	public String getIP(){
		return this.IP;
	}
	
	public int getPort(){
		return this.port;
	}
	
	public String getID(){
		return this.ID;
	}
}
