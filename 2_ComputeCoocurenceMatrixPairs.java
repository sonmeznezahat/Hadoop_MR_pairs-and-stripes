import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


//public class ComputeCoocurenceMatrixPairs {
	public class ComputeCoocurenceMatrixPairs extends Configured implements Tool {
		//private static final Logger LOG = Logger.getAnonymousLogger();

		private static class MyMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {
			private static final PairOfStrings PAIR = new PairOfStrings();
			private static final IntWritable ONE = new IntWritable(1);
			private int window = 2;

		
			
			@Override
			public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
				final int LIMIT = 100;
				int count = 0;
				window = 2;
				String line = ((Text) value).toString();

				ArrayList<String> tokens = new ArrayList<String>();
				StringTokenizer itr = new StringTokenizer(line);
				while (itr.hasMoreTokens()) {
					count++;
					
					String w = itr.nextToken().toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", "");
					if (w.length() == 0)
						continue;
					tokens.add(w);
					
					
					
					if (count > LIMIT)
						break;
					
					PAIR.set("*", "*");
					context.write(PAIR, ONE);
				}
				
				for (int i = 0; i < tokens.size(); i++) {
					for (int j = Math.max(i - window, 0); j < Math.min(i + window + 1, tokens.size()); j++) {
						if (i == j)
							continue;
						if (tokens.get(i).equals(tokens.get(j)))
							continue;

						PAIR.set(tokens.get(i), tokens.get(j));
						context.write(PAIR, ONE);

						PAIR.set(tokens.get(i), "*");
						context.write(PAIR, ONE);

					}
				}
			}
		}
/*
		private static class MyReducer extends Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
			
			private final static IntWritable SUM = new IntWritable();

			@Override
			public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
				
				int sum=0;
				final int LIMIT = 10;
				
				for(IntWritable yy : values){
					sum += yy.get();
				}
				if (sum < LIMIT)
					return;
				
				SUM.set(sum);
				
				context.write(key, SUM);
			}
		}
*/
		private static class MyReducer extends
		Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable>
{
	private static final PairOfStrings PAIR = new PairOfStrings();
	private final static IntWritable VALUE = new IntWritable();
	private int fringy = 0;
	private int noOfTransactions = 0;
	
	@Override
	public void reduce(PairOfStrings key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException
	{
		
		int sum = 0;
		final int LIMIT = 10;


		for(IntWritable yy : values){
			sum += yy.get();
		}

		
		if (key.getLeftElement().equals("*") && key.getRightElement().equals("*"))
		{
			noOfTransactions = sum;
		}
		else if (key.getRightElement().equals("*"))
		{
			fringy = sum;				
		}
		else
		{
			if (sum < LIMIT)
				return;
			
			VALUE.set(sum);
			String right = key.getRightElement();
			right += " ";
			right += Integer.toString(noOfTransactions);
			right += " ";
			right += Integer.toString(fringy);
			PAIR.set(key.getLeftElement(), right);
			context.write(PAIR, VALUE);
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException
	{

	}
}

		
		protected static class MyPartitioner extends Partitioner<PairOfStrings, IntWritable> {
			@Override
			public int getPartition(PairOfStrings key, IntWritable value, int numReduceTasks) {
				return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
			}
		}
		

		
public int run(String[] argv) throws Exception {
		/*
			Args args = new Args();
			CmdLineParser parser = new CmdLineParser(args, (ParserProperties.defaults()).withUsageWidth(100));

			try {
				parser.parseArgument(argv);
			} catch (CmdLineException e) {
				System.err.println(e.getMessage());
				parser.printUsage(System.err);
				return -1;
			}

			LOG.info("Tool: " + ComputeCooccurrenceMatrixPairs.class.getSimpleName());
			LOG.info(" - input path: " + args.input);
			LOG.info(" - output path: " + args.output);
			LOG.info(" - window: " + args.window);
			LOG.info(" - number of reducers: " + args.numReducers);
		*/
	        Configuration conf = getConf();
			Job job = Job.getInstance(conf);
			job.setJobName(ComputeCoocurenceMatrixPairs.class.getSimpleName());
			job.setJarByClass(ComputeCoocurenceMatrixPairs.class);

			// Delete the output directory if it exists already.
			// Path outputDir = new Path(args.output);
			// Path outputDir = new Path(argv[ 1 ]);

			// job.getConfiguration().setInt("window", args.window);
			job.getConfiguration().setInt("window", 100);

			// job.setNumReduceTasks(args.numReducers);
			job.setNumReduceTasks(1);

			// FileInputFormat.setInputPaths(job, new Path(args.input));
			// FileOutputFormat.setOutputPath(job, new Path(args.output));

		    FileInputFormat.setInputPaths(job, new Path(argv[0]));
		    FileOutputFormat.setOutputPath(job, new Path(argv[1]));

			job.setMapOutputKeyClass(PairOfStrings.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(PairOfStrings.class);
			job.setOutputValueClass(IntWritable.class);

			job.setMapperClass(MyMapper.class);
			// job.setCombinerClass(MyReducer.class);
			job.setReducerClass(MyReducer.class);
			job.setPartitionerClass(MyPartitioner.class);

			long startTime = System.currentTimeMillis();
			job.waitForCompletion(true);
			System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

			return 0;
		}



		
	//} 
	
	public static void main(String[] args) throws Exception {
		
	    ToolRunner.run(new ComputeCoocurenceMatrixPairs(), args);
	    
	  }



	

}
