/*
 * Cloud9: A MapReduce Library for Hadoop
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package edu.umd.cloud9.examples;

import edu.umd.cloud9.io.PairOfWritables;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.*;
import java.util.*;

public class JingAnalyzeBigramCount {
	public static void main(String[] args) {

		Path input_Path = new Path("/Users/HUANGWEIJIE/Dropbox/Information Retrieval and Data Mining/individual assignment/Distributed-Computing/MapReduceAssignment/src/train/");
		System.out.println("input path: " + input_Path.toString());

		List<PairOfWritables<Text, IntWritable>> bigrams;
		try {
			bigrams = readDirectory(input_Path);

			Collections.sort(bigrams, new Comparator<PairOfWritables<Text, IntWritable>>() {
				public int compare(PairOfWritables<Text, IntWritable> e1,
						PairOfWritables<Text, IntWritable> e2) {
					if (e2.getRightElement().compareTo(e1.getRightElement()) == 0) {
						return e1.getLeftElement().compareTo(e2.getLeftElement());
					}

					return e2.getRightElement().compareTo(e1.getRightElement());
				}
			});

			int singletons = 0;
			int sum = 0;
			for (PairOfWritables<Text, IntWritable> bigram : bigrams) {
				sum += bigram.getRightElement().get();

				if (bigram.getRightElement().get() == 2) {
					singletons++;
				}
			}

			System.out.println("Total number of unique bigrams is: " + bigrams.size());
			System.out.println("Total number of bigrams: " + sum);
			System.out.println("The number of bigrams that appear twice: " + singletons);

			System.out.println("\nThe top twenty most frequent bigrams and their counts are: ");

			int cnt = 0;
			double fraction=0.0; 
			for (PairOfWritables<Text, IntWritable> bigram : bigrams) {
				System.out.println(bigram.getLeftElement() + "\t" + bigram.getRightElement());
				cnt++;
				fraction=fraction+bigram.getRightElement().get();
				if (cnt >= 20) {
					break;
				}
				
			}
			//System.out.println("The cumulative frequency of the top twenty bigrams is: " + fraction);

			
		} catch (IOException e) {
			System.err.println("Couldn't load folder: " + input_Path.toString());
		}
	}
	

	/**
	 *  Reads in the bigram count file 
	 * @param path
	 * @return
	 * @throws IOException
	 */
	private static List<PairOfWritables<Text, IntWritable>> readDirectory(Path path)
			throws IOException {

		File dir = new File(path.toString());
		ArrayList<PairOfWritables<Text, IntWritable>> bigrams = new ArrayList<PairOfWritables<Text, IntWritable>>();
		for (File child : dir.listFiles()) {
			if (".".equals(child.getName()) || "..".equals(child.getName())) {
				continue; // Ignore the self and parent aliases.
			}
			FileInputStream bigramFile = null;

			bigramFile = new FileInputStream(child.toString());

			// Read in the file
			DataInputStream resultsStream = new DataInputStream(bigramFile);
			BufferedReader results = new BufferedReader(new InputStreamReader(resultsStream));

			StringTokenizer rToken;
			String rLine;
			String firstWord;
			String secondWord;
			String count;

			// iterate through every line in the file
			while ((rLine = results.readLine()) != null) {
				rToken = new StringTokenizer(rLine);
				// extract the meaningful information
				firstWord = rToken.nextToken();
				secondWord = rToken.nextToken();
				count = rToken.nextToken();

				bigrams.add(new PairOfWritables<Text, IntWritable>(new Text(firstWord + " "
						+ secondWord), new IntWritable(Integer.parseInt(count))));

			}
			if (bigramFile != null)
				bigramFile.close();
		}

		return bigrams;

	}

}