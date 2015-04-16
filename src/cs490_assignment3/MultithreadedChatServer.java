package cs490_assignment3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class MultithreadedChatServer {
	static int heartbeat_rate = 1000;
    static ConcurrentHashMap<String, Process> group;
    static ConcurrentHashMap<String, Boolean> hbMap;
    static ConcurrentHashMap<String, PrintWriter> outChannel;
    static ConcurrentHashMap<String, BufferedReader> inChannel;
	final static int numThread = 10;
	
	public static void main(String[] args) {
        ServerSocket serverSocket;
        group = new ConcurrentHashMap<String, Process>();
        hbMap = new ConcurrentHashMap<>();
        outChannel = new ConcurrentHashMap<>();
        inChannel = new ConcurrentHashMap<>();
        
        ExecutorService executorService = Executors.newFixedThreadPool(numThread);
        
        try{
        	serverSocket = new ServerSocket(1234);     
            Timer timer = new Timer();
            timer.schedule(new TimerTask_checkHB(), 0, heartbeat_rate);
            
            while(true){
                Socket clientSocket = serverSocket.accept();
                PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                Runnable r = new MessageHandler( clientSocket,out, in);
                executorService.execute(r);
            }
        }
        catch(Exception e){
        	e.printStackTrace();
        }
        finally{
        	executorService.shutdown();
        }
	}
	

	static class MessageHandler implements Runnable{
		private Socket socket;
		PrintWriter out;
		BufferedReader in;
		String clientID;
		public MessageHandler(Socket clientSocket, PrintWriter out, BufferedReader in ){
			this.socket = clientSocket;
			this.out = out;
			this.in = in;
		}
		
		public void setID(String s){
			this.clientID = s;
		}
		
		public void run(){
			while(!socket.isClosed()){
				try{
	                //System.out.println("Spawn to handle")
	                String message;  // Get message from client
	                message = in.readLine();
	                if (message != null) {
	                	// Split the message by '|'
		                String[] m_info = message.split("\\|");
		                
		                // Handle message
		                if(m_info[0].equals("REGISTER")){
		                    String[] r_info = m_info[1].split(",");
		                    if(group.containsKey(r_info[0])){
		                        // Decline it
		                        out.println("ERROR");
		                        System.out.println("Duplicate name in group!");
		                    }
		                    else{
		                    	setID(r_info[0]);
		                        String temp1[] = socket.getRemoteSocketAddress().toString().split("/");
		                        String temp2[] = temp1[1].split(":");
		                        m_info[1] = new String(temp2[0]);
		                    	
		                    	Process newclient = new Process(m_info[1], Integer.parseInt(r_info[1]), r_info[0]);
		                    	for(Map.Entry<String, PrintWriter> c: outChannel.entrySet()){
		                    		c.getValue().println("New-"+newclient.getID()+","+newclient.getIP()+","+newclient.getPort());
		                    	}
		                    	group.put(r_info[0], newclient);
		                    	outChannel.put(r_info[0], out);
		                    	inChannel.put(r_info[0], in);
		                        hbMap.put(r_info[0], true);
		                        out.println(socket.getRemoteSocketAddress().toString());
		                        //System.out.println("Register successfully");
		                    }
		                }
		                else if(m_info[0].equals("heartbeat")){
		                    // Update correspond clint's last heartbeat time
		                	String name = m_info[1];
		            		if(hbMap.containsKey(name)){
		            			hbMap.put(name, true);
		            			//System.out.println("receive a HEARTBEAT from "+name);
		            		}
		            		else{
		            	//		System.out.println("receive a HEARTBEAT from "+name+", which is not in the group");
		            		}
		                }
		                else if(m_info[0].equals("GET")){
		                    // Send group information back to client
		                    StringBuilder sBuilder = new StringBuilder();             
		                    for(Map.Entry<String, Process> entry : group.entrySet()){
		                        Process m = entry.getValue();
		                        sBuilder.append(m.getID()+","+m.getIP()+","+m.getPort()+"|");
		                    }
		                    // Format: Name,IP,Port|Name,IP,Port|....|Name,IP,Port|
		                    String returnMessage = sBuilder.toString();
		                    
		                    out.println("Group-"+returnMessage);
		                    //System.out.println(returnMessage);
		                }
		                else{
		                    //Invalid message
		                    System.out.println("Invalid message: "+message);
		                }
					}
				}
				catch(Exception e){
					e.printStackTrace();
					try {
						socket.close();
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
			}
			System.out.println("Socket is closed");
		}
	}
	
	
    static class TimerTask_checkHB extends TimerTask{
        
        @Override
        public void run(){
            for(Map.Entry<String, Boolean> entry : hbMap.entrySet()){
        		Boolean b = entry.getValue();
            	if(!b){
            		Process removedProcess = group.get(entry.getKey());
            		System.out.println(entry.getKey()+" is down");
            		String username = entry.getKey();
                	hbMap.remove(username);      
                	group.remove(username);
                	outChannel.remove(username);
                	for(Map.Entry<String, PrintWriter> c: outChannel.entrySet()){                		
                		c.getValue().println("Remove-"+removedProcess.getID()+","+removedProcess.getIP()+","+removedProcess.getPort());
                	}
            	}
            	else{
                	hbMap.put(entry.getKey(), false);
            	}
            }
        }
    }
}
