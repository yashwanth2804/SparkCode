package Collections.Lists;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

public class ArrayLists {
	
	public static class Author implements Comparable<Author> {
		
		public String getN() {
			return n;
		}

		public void setN(String n) {
			this.n = n;
		}

		public Integer getAg() {
			return ag;
		}

		public void setAg(Integer ag) {
			this.ag = ag;
		}

		String n;
		Integer ag;
		
		Author(String name,int age){
			n=name;
			ag=age;	
		}
		
		@Override
		public int compareTo(Author au) {
		 int age1 = ag.compareTo(au.ag);
			return age1;
		}
		
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		ArrayList <String> f = new ArrayList<String>();
		f.add("UI");
		
		// careate array
		String h[] = {"e","b","c","a"};
		
		ArrayList <String> f1 = new ArrayList<String>(Arrays.asList(h));
		 
		for(String h1 : f1)
			System.out.print(h1);
		
		f1.add("d");
		
		f1.addAll(f);
	 
		
		for(String h1 : f1)
			System.out.print(h1);
		
		//convert to iterator 
		
		Iterator it = f1.iterator();
		
		while(it.hasNext())
			System.out.println(it.next());
		
		System.out.println(f1.size());
		System.out.println(f1.isEmpty());
		
		
		Collections.sort(f1);
		
		System.out.println(f1);
		
		
		ArrayList<Author> aut = new ArrayList<Author>();
		aut.add(new Author("G", 100));
		aut.add(new Author("B", 20));
		aut.add(new Author("Y", 60));
		
		Collections.sort(aut);
		
		for(Author f2 : aut)
			System.out.println(f2.getN());
		
		
		
		
	}

}
