package Collections.Maps;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

public class HashMaps {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		HashMap<String,String> hm = new HashMap<String, String>();
		
		hm.put("e","a1");
		hm.put("b","a1");
		hm.put("a","a2");
		 
		hm.put("a","a5");
		
		Set s = hm.entrySet();
		Iterator it = s.iterator();
		
		for(Map.Entry me : hm.entrySet())
			System.out.println(me.getKey()+" "+me.getValue());
		
		
		while(it.hasNext())
		{
			Map.Entry mentry = (Map.Entry) it.next();
			System.out.println(mentry.getKey() +" "+mentry.getValue());
			
		}
		
	Map<String,String> thmp = 	new  TreeMap<String,String> (hm);
	System.out.println();
	for(Map.Entry g : thmp.entrySet())
		System.out.println(g.getKey()+" "+g.getValue());
	
	
	System.out.println(hm.containsKey("a")+" "+hm.containsValue("a5"));
	
		
		
		
	}

}
