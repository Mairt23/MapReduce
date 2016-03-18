package org.mapreduce;

import java.io.*;
import java.util.concurrent.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.lang.*;

public class MapReduceEdited {
    
	//arg1 = number of text files
	//arg2 = number of threads
	//All other args are textfile names
	public static void main(String[] args) {
		long startTime = System.currentTimeMillis();
		
		long total = 0;
      	for (int i = 0; i < 10000000; i++) {
        	total += i;
        }
		
		// the problem:
		
		// from here (INPUT)
		
		// "file1.txt" => "foo foo bar cat dog dog"
		// "file2.txt" => "foo house cat cat dog"
		// "file3.txt" => "foo foo foo bird"

		// we want to go to here (OUTPUT)
		
		// "foo" => { "file1.txt" => 2, "file3.txt" => 3, "file2.txt" => 1 }
		// "bar" => { "file1.txt" => 1 }
		// "cat" => { "file2.txt" => 2, "file1.txt" => 1 }
		// "dog" => { "file2.txt" => 1, "file1.txt" => 2 }
		// "house" => { "file2.txt" => 1 }
		// "bird" => { "file3.txt" => 1 }
		
		// in plain English we want to
		
		// Given a set of files with contents
		// we want to index them by word 
		// so I can return all files that contain a given word
		// together with the number of occurrences of that word
		// without any sorting
		
		////////////
		// INPUT:
		///////////
		
		
		
		
		Map<String, String> input = new HashMap<String, String>();
		
		int numFiles = Integer.parseInt(args[0]);
		int numThreads = Integer.parseInt(args[1]);
		
		for(int i = 0; i < numFiles; i++)
		{
			try
			{
				FileReader inFile = new FileReader(args[i+2]);
				BufferedReader bufferReader = new BufferedReader(inFile);
				String line;
				while ((line = bufferReader.readLine()) != null)
				{
					String temp = input.get(args[i+2]);
					if (temp != null){
						temp = temp + line + " ";
						input.put(args[i+2], temp);
					}
					else{
						input.put(args[i+2], line + " ");
					}
				}
			}
			catch(Exception e)
			{
				System.out.println("Error while reading file line by line:" + e.getMessage());
			}
		}
		
		
		System.out.println("Input File:");
		System.out.println(input);
		
		System.out.println("Method 1:");
		// APPROACH #1: Brute force
		{
				Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();
				
				Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
				while(inputIter.hasNext()) {
						Map.Entry<String, String> entry = inputIter.next();
						String file = entry.getKey();
						String contents = entry.getValue();
						
						String[] words = contents.trim().split("\\s+");
						
						for(String word : words) {
								
								Map<String, Integer> files = output.get(word);
								if (files == null) {
										files = new HashMap<String, Integer>();
										output.put(word, files);
								}
								
								Integer occurrences = files.remove(file);
								if (occurrences == null) {
										files.put(file, 1);
								} else {
										files.put(file, occurrences.intValue() + 1);
								}
						}
				}
				
				// show me:
				System.out.println(output);
				long stopTime = System.currentTimeMillis();
      			long elapsedTime = stopTime - startTime;
      			System.out.printf("Time taken for method 1 in milliseconds: %d\n", elapsedTime);
		}

		System.out.println("Method 2:");
		// APPROACH #2: MapReduce
		{
				Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();
				
				// MAP:
				
				List<MappedItem> mappedItems = new LinkedList<MappedItem>();
				
				Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
				while(inputIter.hasNext()) {
						Map.Entry<String, String> entry = inputIter.next();
						String file = entry.getKey();
						String contents = entry.getValue();
						
						map(file, contents, mappedItems);
				}
				
				// GROUP:
				
				Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();
				
				Iterator<MappedItem> mappedIter = mappedItems.iterator();
				while(mappedIter.hasNext()) {
						MappedItem item = mappedIter.next();
						String word = item.getWord();
						String file = item.getFile();
						List<String> list = groupedItems.get(word);
						if (list == null) {
								list = new LinkedList<String>();
								groupedItems.put(word, list);
						}
						list.add(file);
				}
				
				// REDUCE:
				
				Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
				while(groupedIter.hasNext()) {
						Map.Entry<String, List<String>> entry = groupedIter.next();
						String word = entry.getKey();
						List<String> list = entry.getValue();
						
						reduce(word, list, output);
				}
				
				System.out.println(output);
				long stopTime = System.currentTimeMillis();
      			long elapsedTime = stopTime - startTime;
      			System.out.printf("Time taken for method 2 in milliseconds: %d\n", elapsedTime);
		}
		
		System.out.println("Method 3:");
		
		// APPROACH #3: Distributed MapReduce
		{
				
				
				// MAP:
				//Create CopyOnWriteArrayList
				//final List<MappedItem> mappedItems = new LinkedList<MappedItem>();
				final List<MappedItem> mappedItems = new CopyOnWriteArrayList<MappedItem>();
				//Get rid of this code???
				/*final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {
						@Override
		public synchronized void mapDone(String file, List<MappedItem> results) {
			mappedItems.addAll(results);
		}
				};*/
				
				List<Thread> mapCluster = new ArrayList<Thread>(input.size());
				
				Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
				
				ExecutorService mapThreadPool = Executors.newFixedThreadPool(numThreads);
				
				while(inputIter.hasNext()) {
					//System.out.println("Got here");
					Map.Entry<String, String> entry = inputIter.next();
					final String file = entry.getKey();
					final String contents = entry.getValue();
					mapThreadPool.execute(new Thread(new Runnable() {
						@Override
						public void run() {
							//Pass in CopyOnWriteArrayList instead of mapCallback
							map(file, contents, mappedItems);
						}
					}));
				}
				
				mapThreadPool.shutdown();
				
				while(!mapThreadPool.isTerminated()){}
				
				// GROUP:
				
				Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();
				
				Iterator<MappedItem> mappedIter = mappedItems.iterator();
				while(mappedIter.hasNext()) {
						MappedItem item = mappedIter.next();
						String word = item.getWord();
						String file = item.getFile();
						List<String> list = groupedItems.get(word);
						if (list == null) {
								list = new LinkedList<String>();
								groupedItems.put(word, list);
						}
						list.add(file);
				}
				
				// REDUCE:
				final Map<String, Map<String, Integer>> output = new ConcurrentHashMap<String, Map<String, Integer>>();
				//Modifications (part 3) mirror those of map phase
				
				/*final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {
						@Override
		public synchronized void reduceDone(String k, Map<String, Integer> v) {
				output.put(k, v);
		}
				};*/
				
				List<Thread> reduceCluster = new ArrayList<Thread>(groupedItems.size());
				
				Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
				
				ExecutorService reduceThreadPool = Executors.newFixedThreadPool(numThreads);
				
				while(groupedIter.hasNext()){
					Map.Entry<String, List<String>> entry = groupedIter.next();
					final String word = entry.getKey();
					final List<String> list = entry.getValue();
					reduceThreadPool.execute(new Thread(new Runnable() {
						@Override
						public void run() {
							reduce(word, list, output);
						}
						}));
				}
				
				reduceThreadPool.shutdown();
				
				while(!reduceThreadPool.isTerminated()){}
				
				System.out.println(output);
				long stopTime = System.currentTimeMillis();
      			long elapsedTime = stopTime - startTime;
      			System.out.printf("Time taken for method 3 in milliseconds: %d\n", elapsedTime);
		}
}

	public static void map(String file, String contents, List<MappedItem> mappedItems) {
			String[] words = contents.trim().split("\\s+");
			for(String word: words) {
					mappedItems.add(new MappedItem(word, file));
			}
	}
	
	public static void reduce(String word, List<String> list, Map<String, Map<String, Integer>> output) {
			Map<String, Integer> reducedList = new HashMap<String, Integer>();
			for(String file: list) {
					Integer occurrences = reducedList.get(file);
					if (occurrences == null) {
							reducedList.put(file, 1);
					} else {
							reducedList.put(file, occurrences.intValue() + 1);
					}
			}
			output.put(word, reducedList);
	}
	
	//got rid of mapcallback.  
	
	public static void map(String file, String contents, CopyOnWriteArrayList<MappedItem> mappeditems) {
			String[] words = contents.trim().split("\\s+");
			List<MappedItem> results = new ArrayList<MappedItem>(words.length);
			for(String word: words) {
					results.add(new MappedItem(word, file));
			}
			//Just call CopyOnWriteArrayList.add
			//callback.mapDone(file, results);
			mappeditems.addAll(results);
	}
	
	public static interface ReduceCallback<E, K, V> {
			
			public void reduceDone(E e, Map<K,V> results);
	}
	
	public static void reduce(String word, List<String> list, ConcurrentHashMap<String, Map<String, Integer>> output) {
			
			Map<String, Integer> reducedList = new HashMap<String, Integer>();
			for(String file: list) {
					Integer occurrences = reducedList.get(file);
					if (occurrences == null) {
							reducedList.put(file, 1);
					} else {
							reducedList.put(file, occurrences.intValue() + 1);
					}
			}
			output.put(word, reducedList);
	}
	
	private static class MappedItem { 
			
			private final String word;
			private final String file;
			
			public MappedItem(String word, String file) {
					this.word = word;
					this.file = file;
			}

			public String getWord() {
					return word;
			}

			public String getFile() {
					return file;
			}
			
			@Override
			public String toString() {
					return "[\"" + word + "\",\"" + file + "\"]";
			}
	}
} 

