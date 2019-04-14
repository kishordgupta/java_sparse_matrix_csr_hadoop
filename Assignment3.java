package com.wordcount;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Hashtable;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Assignment3 extends Configured implements Tool {

	public static class ColumnMapper extends Mapper<Object, Text, Text, Text> {

		static Hashtable<Integer, String> valueswithcolumn = new Hashtable<Integer, String>();
		
		void setips(Context context) throws UnsupportedEncodingException {

			Path datafile = new Path("\\mapreduce\\assignment3\\data");
			Path cindex = new Path("\\mapreduce\\assignment3\\cindx");

			FileSystem fs = null;
			try {
				fs = datafile.getFileSystem(context.getConfiguration());

				FSDataInputStream inputpathstreamdatafile = fs.open(datafile);

				InputStreamReader isra = new InputStreamReader(inputpathstreamdatafile, "ISO-8859-1");
				;

				BufferedReader dbr = new BufferedReader(isra);
				Scanner scanner = new Scanner(dbr);

				System.out.print("01 ");
				System.gc();

				FileSystem dfs = cindex.getFileSystem(context.getConfiguration());
				FSDataInputStream inputpathstreamcindex = dfs.open(cindex);

				InputStreamReader isr = new InputStreamReader(inputpathstreamcindex, "ISO-8859-1");
				BufferedReader br = new BufferedReader(isr);
				Scanner scanner1 = new Scanner(br);

				int ii = 0;
				while (scanner.hasNext()) {

					valueswithcolumn.put(ii, scanner.next() + "," + scanner1.next());

					ii++;
				}

				scanner.close();
				dbr.close();
				isra.close();
				inputpathstreamdatafile.close();
				scanner1.close();
				br.close();
				isr.close();

				inputpathstreamcindex.close();

				System.out.print("02 ");
				System.gc();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			System.gc();
			System.out.print("004 ");
		}

		protected void setup(Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
			setips(context);
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			StringTokenizer Rowtoken = new StringTokenizer(value.toString());
			Rowtoken.nextToken();
			int offset = 0;
			int newmatrixindex = 0;
			System.out.print("1 ");
			while (Rowtoken.hasMoreTokens()) {
				int keyd = Integer.parseInt(Rowtoken.nextToken().toString());

				for (; offset < keyd; offset++) {
					context.write(new Text("" + newmatrixindex), new Text(valueswithcolumn.get(offset) + ""));

				}
				newmatrixindex++;
			}
			System.out.print("2 ");

			Rowtoken = null;
			System.gc();
		}

	}

	public static class ColumnReducer extends Reducer<Text, Text, Text, Text> {

		static String[] xvecr;

		@Override
		protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);

			Path secmatrix = new Path("\\mapreduce\\assignment3\\xvector");
			FileSystem fs = secmatrix.getFileSystem(context.getConfiguration());
			FSDataInputStream inputpathstreamsecmatrix = fs.open(secmatrix);
			InputStreamReader isra = new InputStreamReader(inputpathstreamsecmatrix, "ISO-8859-1");
			;
			BufferedReader dbr = new BufferedReader(isra);
			xvecr = dbr.readLine().split(" ");
			dbr.close();
			isra.close();
			inputpathstreamsecmatrix.close();
			System.gc();

		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			int value = 0;
			for (Text g : values) {
				String[] data = g.toString().split(",");

				int avalue = Integer.parseInt(data[0]);
				int acvalue = Integer.parseInt(data[1]);
				int bvalue = Integer.parseInt(xvecr[acvalue]);
				value = value + (avalue * bvalue);
			}

			context.write(key, new Text(value + ""));

		}

	}

	public static void main(String[] args) throws Exception {

		ToolRunner.run(new Assignment3(), args);

	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub

		/////////
		Path rindex = new Path("\\mapreduce\\assignment3\\rindx");
		Path output = new Path("\\output\\assignment1\\");
		///////////
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(output))
			hdfs.delete(output, true);

		Job job = Job.getInstance(conf, "sparse");
		job.setJarByClass(Assignment3.class);
		job.setMapperClass(ColumnMapper.class);
		job.setCombinerClass(ColumnReducer.class);
		job.setReducerClass(ColumnReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, rindex);
		FileOutputFormat.setOutputPath(job, output);
		job.waitForCompletion(true);

		return 1;
	}

}
