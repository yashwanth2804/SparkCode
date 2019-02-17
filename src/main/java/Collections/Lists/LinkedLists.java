package Collections.Lists;

import java.util.Iterator;
import java.util.LinkedList;

public class LinkedLists {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		// linked lists are linear data structure 
		// linked with pointer and nodes
		// implements List and queue
		// https://beginnersbook.com/wp-content/uploads/2013/12/Java-collection-framework-hierarchy.png
		
		LinkedList<String> llL = new LinkedList<String>();
		
		llL.add("a1");
		llL.add("a2");
		
		System.out.println(llL.get(0));
			Object y = 	llL.clone();
	
		System.out.println(y);
		 
		Iterator<String> g = llL.iterator();
		
		while(g.hasNext())
			System.out.println(g.next());
		
	}

}
