package cs490_assignment3;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class CasualReliableBroadcastImp implements CasualReliableBroadcast{
	private HashMap<String, Process> group;
	private static HashSet<Message> deliveredSet;
	private static HashMap<String, PrintWriter> pwMap;
	private static HashMap<String, BufferedReader> brMap;
	private static HashMap<String, Integer> lastSeqMap;	//should delete
	private static HashMap<String, PriorityQueue<Message>> pendingMap; //is message or messageimp
	int numThread =15;
	private Process currentProcess;
	private BroadcastReceiver br;
	final ExecutorService executorService = Executors.newFixedThreadPool(numThread);
	public VectorClock self_vc;  //self vc, should maintain an vc or ConcurrentHashMap


	@Override
	public void init(final Process currentProcess, final BroadcastReceiver br) {
		this.currentProcess = currentProcess;
		this.br = br;
		group = new HashMap<>();
		deliveredSet = new HashSet<>();
		pwMap = new HashMap<>();
		brMap = new HashMap<>();
		lastSeqMap = new HashMap<>();
		pendingMap = new HashMap<>();
		self_vc = new VectorClock(currentProcess.getID()+"+0"); // initialize local VC(only contain self id)
		
		// Create a thread to listen to the port to keep saving new ois
		Thread connectionAccepter = new Thread(new Runnable(){
			@Override
			public void run() {
				ServerSocket serverSocket;
				try {
					serverSocket = new ServerSocket(currentProcess.getPort());
					while(true){
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
		for(PrintWriter pWriter : pwMap.values()){
			try {
				//the message that pass in should contain the vc
				pWriter.println(message.toString());
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(3);
			};
		}

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
					String messageString = in.readLine();
					Message receiveMessage = new MessageImp(messageString);
					
					synchronized (deliveredSet) {
						if(!deliveredSet.contains(receiveMessage)){
							//System.out.println("Checking");
							deliveredSet.add(receiveMessage);

							String[] info = receiveMessage.getMessageContents().split(":");
							String senderName = info[0];
							tmp_vc = receiveMessage.getMessageVC();
							
							if(currentProcess.getID().equals(senderName)){ //message sender
								deliveredSet.add(receiveMessage);
								self_vc.increment(senderName);
							}
							else{ //message from other process
								//this < vc pending, this = vc deliver, this > vc ignore 
								if(self_vc.compareTo(tmp_vc) == 0){ // this = vc deliver, 
									// Should crDeliver the message, update the map
									br.receive(receiveMessage);
									crbroadcast(receiveMessage);

									if(pendingMap.containsKey(senderName)){
										// checking the pending map and deliver all messages that should be delivered
										PriorityQueue<Message> pq = pendingMap.get(senderName);
										while(pq.peek()!=null && 
												pq.peek().getMessageVC().getVectorValue(senderName) == (self_vc.getVectorValue(senderName)+1)){
											receiveMessage = pq.poll();
											//System.out.println("Redeliver message "+receiveMessage.getMessageNumber());
											br.receive(receiveMessage);
											crbroadcast(receiveMessage);
											deliveredSet.add(receiveMessage);	
										}
									}
								}
								else if(self_vc.compareTo(tmp_vc) == -1){ //this < vc pending
									// Put message into the pending map
									//System.out.println("Pending message "+receiveMessage.getMessageNumber());
									if(!pendingMap.containsKey(senderName)){
										// Need to initialize a priority queue
										pendingMap.put(senderName, new PriorityQueue<Message>());
									}
									pendingMap.get(senderName).add(receiveMessage);
								}
							
							}
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
