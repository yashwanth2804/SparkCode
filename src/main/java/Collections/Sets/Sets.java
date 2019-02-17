package Collections.Sets;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.TreeSet;

public class Sets {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// There is unknown order - no gaurante order
		
		HashSet<String> hs = new HashSet<String>();
 hs.add("A");
 hs.add("B");
 hs.add("BBR");
 hs.add("A");
 
 for(String h : hs) 
	 System.out.println(h);
 
 hs.clear();
  
 System.out.println(hs.contains("B"));
 
 
 // there is an order - ascending
 
  TreeSet<String> ts  = new TreeSet<String>();
  ts.add("A");
  ts.add("C");
  ts.add("BBR");
  ts.add("A");
  
  for(String h : ts)
	  System.out.println(h);
  
  //Linkedhashset
  
 // mainain in provider order
  
 LinkedHashSet<String> lhs = new LinkedHashSet<String>();
 
 lhs.add("w");
 lhs.add("t");
 
 
 
	}

}
