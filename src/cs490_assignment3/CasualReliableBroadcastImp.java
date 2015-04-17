package cs490_assignment3;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class CasualReliableBroadcastImp implements CasualReliableBroadcast{
	private HashMap<String, Process> group;
	private static Set<Message> deliveredSet;
	public static ConcurrentHashMap<String, Message> deliverSet;
	private static HashMap<String, PrintWriter> pwMap;
	private static HashMap<String, BufferedReader> brMap;
	private static HashMap<String, Integer> lastSeqMap;	//should delete
	private static PriorityQueue<Message> pendingQueue; //is message or messageimp
	int numThread =15;
	private Process currentProcess;
	private BroadcastReceiver br;
	final ExecutorService executorService = Executors.newFixedThreadPool(numThread);
	public VectorClock self_vc;  //self vc, should maintain an vc or ConcurrentHashMap
	public static Integer lock = new Integer(0);


	@Override
	public void init(final Process currentProcess, final BroadcastReceiver br) {
		this.currentProcess = currentProcess;
		this.br = br;
		group = new HashMap<>();
		deliveredSet = Collections.synchronizedSet(new HashSet<Message>());
		deliverSet = new ConcurrentHashMap<>();
		pwMap = new HashMap<>();
		brMap = new HashMap<>();
		lastSeqMap = new HashMap<>();
		pendingQueue = new PriorityQueue<>();
		self_vc = new VectorClock(currentProcess.getID()+"+0"); // initialize local VC(only contain self id)
		
		// Create a thread to listen to the port to keep saving new ois
		Thread connectionAccepter = new Thread(new Runnable(){
			@Override
			public void run() {
				ServerSocket serverSocket;
				try {
					serverSocket = new ServerSocket(currentProcess.getPort());
					while(true){
						if(serverSocket.isClosed()){
							System.out.println(currentProcess.getID()+ "serversocket is closed");
						}
						Socket receiveSocket = serverSocket.accept();
						BufferedReader in = new BufferedReader(new InputStreamReader(receiveSocket.getInputStream()));
						String messageString = in.readLine();
						Message message = new MessageImp(messageString);
						if(!brMap.containsKey(message.getMessageContents())){
							brMap.put(message.getMessageContents(), in);
							//System.out.println("Get connection from: " + message.getMessageContents());
							Runnable oisListener = new OISListener(in);
							executorService.execute(oisListener);
							//System.out.println("Create a new thread to listen ois!");
						}
						else{
							System.out.println("Duplicate ois !!!!!");
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}

			}
		});		
		connectionAccepter.start();


	}

	@Override
	public void addMember(Process member) {
		group.put(member.getID(), member);
		if(!pwMap.containsKey(member.getID())){
			try {
				//System.out.println(member.getID()+":member ip = "+ member.getIP() + "member port = "+ member.getPort());
				Socket sendSocket = new Socket(member.getIP(), member.getPort());
				PrintWriter out = new PrintWriter(sendSocket.getOutputStream(), true);
				pwMap.put(member.getID(), out);
			    self_vc.addNewProcess(member.getID());
				// Write the name to receiver, let it put ois into hash map
				Message m = new MessageImp(this.currentProcess.getID()+"|"+self_vc.toString()); //only send the name but no vc or int to identify
				out.println(m.toString());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void removeMember(Process member) {
		group.remove(member.getID());
		brMap.remove(member.getID());
		pwMap.remove(member.getID());
	}

	
	@Override
	public void crbroadcast(Message message) {
		//System.out.println("Process:"+currentProcess.getID()+"-"+ "crboadcast is called");
		String[] info = message.getMessageContents().split(":");
		String senderName = info[0];
		for(Map.Entry<String,PrintWriter> entry : pwMap.entrySet()){
			if(senderName.equals(entry.getKey()) && currentProcess.getID().equals(senderName))
			{
				/*synchronized (lock) {
					//System.out.println("hashcode1:" +((MessageImp)message).hashCode());
					if((!deliveredSet.contains(message))){
						if(message.getMessageContents().equals("EXIT")){
							break;
						}
						//System.out.println("deliveredSet.contains(message)1 = "+ deliveredSet.contains(message));
						deliveredSet.add(message);
						System.out.print("receive1: ");
						br.receive(message);
						System.out.println("Deliver message1 ="+message.getMessageVC().toString()+"|VC ="+ self_vc.toString());
						//System.out.println("deliveredSet.contains(message)2 = "+ deliveredSet.contains(message));

					}
				}*/
				if(!deliverSet.containsKey(message.getMessageVC().toString())){
					if(message.getMessageContents().equals("EXIT")){
						break;
					}
					//System.out.println("deliverSet.containsValue(message)1  "+ deliverSet.containsValue(message));
					deliverSet.putIfAbsent(message.getMessageVC().toString(), message);
					//System.out.print("receive1: ");
					br.receive(message);
					System.out.println("Deliver message1 ="+message.getMessageVC().toString()+"|VC ="+ self_vc.toString());
					//System.out.println("deliveredSet.contains(message)2 = "+ deliveredSet.contains(message));

				}
			}
			else{
				try {	
					//the message that pass in should contain the vc
					entry.getValue().println(message.toString());
				} catch (Exception e) {
					e.printStackTrace();
					System.exit(3);
				};
			}
		}
		self_vc.increment(senderName);
	}

	class OISListener implements Runnable{
		private BufferedReader in;

		public OISListener(BufferedReader in){
			this.in = in;
		}

		@Override
		public void run() {
			VectorClock tmp_vc; // vector clock from other process
			while(brMap.containsValue(in)){
				try{
					Thread.sleep(50);
					String messageString = in.readLine();
					Message receiveMessage = new MessageImp(messageString);
					if(receiveMessage.getMessageContents().equals("EXIT")){
						break;
					}
					
					/*synchronized (lock) {
						//System.out.println("hashcode2:" +((MessageImp)receiveMessage).hashCode());
						if(!deliveredSet.contains(receiveMessage)){
							//System.out.println("deliveredSet.contains(receiveMessage)= "+ deliveredSet.contains(receiveMessage));
							deliveredSet.add(receiveMessage);

							String[] info = receiveMessage.getMessageContents().split(":");
							String senderName = info[0];
							tmp_vc = receiveMessage.getMessageVC();

							//this < vc pending, this = vc deliver, this > vc ignore 
							if(self_vc.compareTo(tmp_vc) >= 0){ // this = vc deliver, 
								// Should crDeliver the message, update the map
								System.out.print("receive2: ");
								br.receive(receiveMessage);
								System.out.println("Deliver message2 "+receiveMessage.getMessageVC().toString()+"|VC ="+ self_vc.toString());
								crbroadcast(receiveMessage);

								if(!pendingQueue.isEmpty()){
									// checking the pending map and deliver all messages that should be delivered
									while(pendingQueue.peek()!=null&&
											self_vc.compareTo(pendingQueue.peek().getMessageVC())>=0){
										receiveMessage = pendingQueue.poll();
										String[] in_fo = receiveMessage.getMessageContents().split(":");
										senderName = in_fo[0];
										//System.out.println("Redeliver message "+receiveMessage.getMessageNumber());
										//System.out.print("receive3: ");
										br.receive(receiveMessage);
										crbroadcast(receiveMessage);
										self_vc.increment(senderName);
										deliveredSet.add(receiveMessage);	
									}
								}
							}
							else if(self_vc.compareTo(tmp_vc) == -1){ //this < vc pending
								// Put message into the pending map
								System.out.println("Pending message "+receiveMessage.getMessageVC().toString()+"|VC ="+ self_vc.toString());
								pendingQueue.add(receiveMessage);								
							}

						}

					}*/
					
					if(!deliverSet.containsKey(receiveMessage.getMessageVC().toString())){
						//System.out.println("deliverSet.containsValue(receiveMessage)= "+deliverSet.containsValue(receiveMessage));
						deliverSet.putIfAbsent(receiveMessage.getMessageVC().toString(), receiveMessage);

						String[] info = receiveMessage.getMessageContents().split(":");
						String senderName = info[0];
						tmp_vc = receiveMessage.getMessageVC();

						//this < vc pending, this = vc deliver, this > vc ignore 
						if(self_vc.compareTo(tmp_vc) >= 0){ // this = vc deliver, 
							// Should crDeliver the message, update the map
							//System.out.print("receive2: ");
							br.receive(receiveMessage);
							System.out.println("Deliver message2 "+receiveMessage.getMessageVC().toString()+"|VC ="+ self_vc.toString());
							crbroadcast(receiveMessage);

							if(!pendingQueue.isEmpty()){
								// checking the pending map and deliver all messages that should be delivered
								while(pendingQueue.peek()!=null&&
										self_vc.compareTo(pendingQueue.peek().getMessageVC())>=0){
									receiveMessage = pendingQueue.poll();
									String[] in_fo = receiveMessage.getMessageContents().split(":");
									senderName = in_fo[0];
									//System.out.println("Redeliver message "+receiveMessage.getMessageNumber());
									//System.out.print("receive3: ");
									br.receive(receiveMessage);
									crbroadcast(receiveMessage);
									self_vc.increment(senderName);
									deliverSet.put(receiveMessage.getMessageVC().toString(), receiveMessage);
								}
							}
						}
						else if(self_vc.compareTo(tmp_vc) == -1){ //this < vc pending
							// Put message into the pending map
							System.out.println("Pending message "+receiveMessage.getMessageVC().toString()+"|VC ="+ self_vc.toString());
							pendingQueue.add(receiveMessage);								
						}

					}
				}
				catch(Exception e){
					e.printStackTrace();
					System.exit(2);
				}
			}
		}

	}

	public HashMap<String, Process> getGroup() {
		// TODO Auto-generated method stub
		return this.group;
	}

}
