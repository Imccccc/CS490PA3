package cs490_assignment3;

import java.io.*;
import java.net.*;
import java.security.acl.Group;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;


public class ChatClient implements BroadcastReceiver{
    static Socket sendSocket;
    static String contact;
    static CasualReliableBroadcastImp crBroadcast;
    static boolean senderflag = false;
    static boolean throughputOut = false;
    static AtomicInteger count = new AtomicInteger(0);


    
    public static void main(String[] args) throws IOException {
        String hostName = "localhost";
        int serverPort = 1234;
        int heartbeat_rate = 1000;
        int clientPort = 0;
        boolean otherHost = false;
        
        if(args.length == 1){
        	clientPort = Integer.parseInt(args[0]);
        }
        else if(args.length > 1){
        	for(String s : args){
        		if(otherHost){
        			hostName = s;
        			otherHost = false;
        			continue;
        		}        		
        		if(s.equals("-s")){
        			senderflag = true;
            		System.out.println("Sender flag is on");
        		}
				else if(s.equals("-t")){
					throughputOut = true;
            		System.out.println("Throughput is on");
				}
				else if(s.equals("-h")){
					otherHost = true;
				}
        		else{
		        	clientPort = Integer.parseInt(s);
				}
        	}
        }
        else{            
        	System.err.println("Usage: java ChatClient [-s] [-t] [-h <address of the host>] <port number>");
        	System.exit(1);
        }
        
        Timer timer = new Timer();
        Timer timer2 = new Timer();

        Socket echoSocket;	// Socket used to connect with server
        PrintWriter out;
        BufferedReader in;
        BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("What is your User name?");
        crBroadcast = new CasualReliableBroadcastImp();
        
        String username;
        String selfIPString = "localhost";
        username = stdIn.readLine();
        assert(username.isEmpty() == false);
        String serverMessage;
        
        try{
            // Initialization
            echoSocket = new Socket(hostName, serverPort);
            out = new PrintWriter(echoSocket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(echoSocket.getInputStream()));
            

            	crBroadcast.init(new Process(selfIPString,  clientPort, username), new ChatClient());
         
            //System.out.println("After Initialization");

            
            // Register
            out.println("REGISTER|"+username+","+clientPort);
            System.out.println("<Waiting server response.....>");
            
            serverMessage = in.readLine();
            if(serverMessage!=null && serverMessage.equals("ERROR")){ // check the server state
                System.err.println("<Username already be registed by other User!>");
                System.exit(1);
            }
            else{
            	// Server will return the IP if registration success
            	selfIPString = serverMessage;
            	System.out.println("<Registration success!>");
            }
            
            
            // Get a copy of the current group
            out.println("GET|List group");
            messageHandler handler = new messageHandler(echoSocket, out, in);
            handler.start();
            
            // Use timer to send heartbeat every heartbeat_rate milliseconds
            TimerTask_heartbeat task = new TimerTask_heartbeat(out, in, username);
            task.setID(username);
            timer.schedule(task, 0, (heartbeat_rate-100)); 
            
            // Output throughput only when "-t" mode
            if(throughputOut){
            	printCount th1 = new printCount(count);
            	timer2.schedule(th1, 0 , 1000);
            }
            
            int seq = 0;
            if (senderflag) {
            	Thread.sleep(3000);
	            for (int i=0; i < 10000; i++) {
	            	//Thread.sleep(50);
	                Message m = new MessageImp(username+": this is message No."+i, crBroadcast.self_vc); // change to (string, vector)
	                crBroadcast.crbroadcast(m);
	            }	
			}
            else{	// Not a dummy sender client
            	String userInput;
            	while(true){
            		userInput = stdIn.readLine();
            		if(userInput.isEmpty())	continue;
            		if(userInput.equals("GET")){
            			HashMap<String, Process> group = new HashMap<String, Process>();
            			group = crBroadcast.getGroup();

    	            	System.out.println("**************Group List**************");
                        System.out.println("There are "+ group.size() +" members");
                        System.out.println("Username\t"+"IP adress\t"+"Port");
                        for(Process p : group.values()){
    	                    System.out.println(p.getID()+"\t\t"+ p.getIP()+"\t"+p.getPort());                        
                        } 
                        System.out.println("************Group List Done************");
            		}
            		else if(userInput.equals("EXIT")){
            			Message m = new MessageImp("EXIT", crBroadcast.self_vc);// change to (string, vector)
            			crBroadcast.crbroadcast(m);
            			handler.Stop();
        				task.Stop();
        				System.exit(0);
            		}
            		else{
            			Message m = new MessageImp(username+": "+userInput, crBroadcast.self_vc);// change to (string, vector)
            			crBroadcast.crbroadcast(m);
            		}
            	}
            }
            
        } catch (Exception e) {
        	e.printStackTrace();
            System.exit(1);
        }
    }
    
    static class TimerTask_heartbeat extends TimerTask{
        String hostName = "localhost";
        int serverPort = 1234;
        PrintWriter out;
        BufferedReader in;
        String username;
        boolean stop = false;
        
    	public TimerTask_heartbeat(PrintWriter out, BufferedReader in, String s){
    		this.out = out;
    		this.in = in;
    		this.username = s;
    	}
    	
        public void Stop() {
        	stop = true;
        }
        
    	public void setID(String s){
    		this.username = s;
    	}
        @Override
        public void run() {
            try{
            	while(!stop){
	                //System.out.println("heartbeat|"+username);
	                out.println("heartbeat|"+username);
	                //sendGetRequest(hostName, serverPort, false);
                }
            }
            catch(Exception e){
    			e.printStackTrace();
            }
        }
    }
    
    static class messageHandler extends Thread{
        String hostName = "localhost";
        int serverPort = 1234;
        PrintWriter out;
        BufferedReader in;
        String serverMessage;
        Socket socket;
        boolean stop = false;
        
    	public messageHandler(Socket socket, PrintWriter out, BufferedReader in){
    		this.out = out;
    		this.in = in;
    		this.socket = socket;
    	}
    	
        public void Stop() {
        	stop = true;
        }
    	
    	public void run(){
    		try{
    			if (socket==null || socket.isClosed()) {
					return;
				}
    			while(!stop && (!socket.isClosed()) &&(serverMessage = in.readLine())!= null){
    	            String[] temp = serverMessage.split("\\-");
    	            //System.out.println(serverMessage+" Server message: "+ temp[0] + " " +temp[1]);
    	            if(temp[0].equals("New")){
    	            	//add memeber;
    	            	String[] m_info = temp[1].split("\\,");
    	                crBroadcast.addMember(new Process(m_info[1], Integer.parseInt(m_info[2]), m_info[0]));
    	            	System.out.println("<"+m_info[0]+" joins the chat group>");
    	            }
    	            else if(temp[0].equals("Remove")){
    	            	//remove member;
        	            String[] m_info = temp[1].split("\\,");
    	            	System.out.println("<"+m_info[0]+" leaves the chat group>");
    	                crBroadcast.removeMember(new Process(m_info[1], Integer.parseInt(m_info[2]), m_info[0]));
    	            }
    	            else if(temp[0].equals("Group")){
    	            	String[] r_info = temp[1].split("\\|");
    	            	System.out.println("**************Group List**************");
    	                System.out.println("There are "+r_info.length+" processs");
    	                System.out.println("Username\t"+"IP adress\t"+"Port");
    	                for(String s : r_info){
    	                    //System.out.println(s);
    	                    String m_info[] = s.split(",");
    	                    String temp1[] = m_info[1].split("/");
    	                    m_info[1] = new String(temp1[0]);
    	                    System.out.println(m_info[0]+"\t\t"+ m_info[1]+"\t"+m_info[2]);
    	                    //System.out.printf("crboadcase.addMember(new Process(%s, %s, %s))\n", m_info[1], Integer.parseInt(m_info[2]), m_info[0]);
    	                    crBroadcast.addMember(new Process(m_info[1], Integer.parseInt(m_info[2]), m_info[0]));
    	                }
    	                System.out.println("************Group List Done************");
    	            }
    			}
    			System.out.println("<Server Down>");
    		}
    		catch(IOException e){
    			e.printStackTrace();
    		}
    		catch(Exception e){
    			System.out.println("messageHandler exception");
    			e.printStackTrace();
    		}
    	}
    }

	@Override
	public void receive(Message m) {
		System.out.println(m.getMessageContents());
		count.incrementAndGet();
	}
	
	
	static class printCount extends TimerTask{
		 	AtomicInteger i;
	        
	    	public printCount(AtomicInteger i){
	    		this.i = i;
	    	}

	        @Override
	        public void run() {
	        	System.err.println("Throughput: "+i.get());
	        	i.set(0);
	        }
	    }
}
