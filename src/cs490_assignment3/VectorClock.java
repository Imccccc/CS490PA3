package cs490_assignment3;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


public class VectorClock{
	private ConcurrentHashMap<String, Integer> vcMap = new ConcurrentHashMap<>();

	public VectorClock(String vcString){
		String[] entryStrings = vcString.split("\\\\");
		for(String entryString : entryStrings){
			String[] info = entryString.split("\\+");
			vcMap.putIfAbsent(info[0], Integer.parseInt(info[1]));
		}
	}

	public void increment(String processName){
		if(vcMap.putIfAbsent(processName, 1) != null){
			Integer count = vcMap.get(processName);
			vcMap.put(processName, count+1);
			count = vcMap.get(processName);
			//System.out.println("VectorClock: "+ processName + " "+ count);

		}
	}

	/* 
	 * return 1  if this > vc
	 * return 0  if this = vc or this || vc
	 * return -1 if this < vc
	 */
	public int compareTo(VectorClock vc) { 
		ConcurrentHashMap<String, Integer> compareMap = vc.getMap();

		Set<String> keySet = new HashSet<>();
		keySet.addAll(compareMap.keySet());
		keySet.addAll(vcMap.keySet());
		boolean greater = false, smaller = false;
		String[] keyArray = new String[keySet.size()];
		keySet.toArray(keyArray);
		for(String key : keyArray){
			Integer count, compareCount;
			count = vcMap.get(key);
			compareCount = compareMap.get(key);
			if(count == null){
				if(compareCount > 0){
					smaller = true;
				}
			}
			else if(compareCount == null){
				if(count > 0){
					greater = true;
				}
			}
			else{
				if(count < compareCount)  smaller = true;
				else if(count > compareCount) greater = true;
			}
		}
		if(greater == smaller)	return 0;
		else if(greater == true) return 1;
		else return -1;
	}
	
	@Override
	public boolean equals(Object obj){
		if (!(obj instanceof VectorClock))
			return false;	
		if (obj == this)
			return true;
		
		ConcurrentHashMap<String, Integer> compareMap = ((VectorClock) obj).getMap();

		Set<String> keySet = new HashSet<>();
		keySet.addAll(vcMap.keySet());
		keySet.addAll(compareMap.keySet());
		String[] keyArray = new String[keySet.size()];
		keySet.toArray(keyArray);
		for(String key : keyArray){
			Integer count, compareCount;
			count = vcMap.get(key);
			compareCount = compareMap.get(key);
			if(count == null || compareCount == null){
				return false;
			}
			else{
				if(count != compareCount)	return false;
			}
		}
		return true;

	}
	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		for(Map.Entry<String, Integer> entry : vcMap.entrySet()){
			sb.append('\\');
			sb.append(entry.getKey());
			sb.append("+");
			sb.append(entry.getValue());
		}
		sb.deleteCharAt(0);
		return sb.toString();
	}

	public ConcurrentHashMap<String, Integer> getMap(){
		return this.vcMap;
	}
	
	public boolean addNewProcess(String processName){
		vcMap.putIfAbsent(processName, 0);
		return true;
	}
	
	public int getVectorValue(String processName){
		if(vcMap.contains(processName)){
			return vcMap.get(processName);
		}
		return 0;
	}
	
	public int setVectorValue(String processName, int value){
		if(vcMap.putIfAbsent(processName, 1) != null){
			Integer count = vcMap.get(processName); // should be count+1?
			vcMap.put(processName, count);
		}
		return 0;
	}
}
