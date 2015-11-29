import java.io.Console;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;

import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonReader;

import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.uima.postag.POSTagger;
import opennlp.tools.util.InvalidFormatException;

public class task1 {

	public static class TokenizerMapper
    extends Mapper<Object, Text, Text, Text>{
		
		List<String> pos_words = Arrays.asList("good","awesome","great","better","nice");
		List<String> bad_words = Arrays.asList("bad","worst","never","terrible","pathetic");
 
 protected void setup(Context context) throws IOException, InterruptedException{
 	
 }

 public void map(Object key, Text value, Context context
                 ) throws IOException, InterruptedException {
 System.out.println("------------------------------------------------");     
	 try {
		JSONObject json = new JSONObject(value.toString());
		String user_id = json.getString("user_id");
		String text  = json.getString("text");
		String review_id = json.getString("review_id");
		System.out.println("{user_id : "+user_id+",  review_id : "+review_id+",  text : "+text+"}");
		
		try {
			InputStream modelIn = new FileInputStream("en-token.bin");
			TokenizerModel tmodel = new TokenizerModel(modelIn);
			Tokenizer tokenizer = new TokenizerME(tmodel); 
			String tokens[] = tokenizer.tokenize(text);
			//creating tagger start
			
			InputStream posmodelIn = new FileInputStream("en-pos-maxent.bin");
			POSModel model = new POSModel(posmodelIn);
			POSTaggerME tagger = new POSTaggerME(model);
			String tags[] = tagger.tag(tokens);
			
			// creating tagger end
			
			// filtering adjectives start
			List<String> adj = new ArrayList<String>();
			int j = 0;
			for(int i = 0;i<tags.length;i++){
				if(tags[i].equals("JJ")){
					adj.add(tokens[i]);		
				}
			}	
			System.out.println("**********Adjective*****************");
			System.out.println(adj.toString());
			//filtering adjectives end
			
			// count good and bad words start
			int pos_count= 0 ;
			int neg_count =0;
			 for(int i =0; i< adj.size();i++){
				 String word = adj.get(i);			 
				 if(pos_words.contains(word)){
					 pos_count +=1;
				 }
				 if(bad_words.contains(word)){
					 neg_count +=1;
				 }				 
			 }
			System.out.println("***************Count*******************");
			System.out.println("Positive words: "+pos_count+" Negative words: "+neg_count);
			
			// count good and bad words end
			
			
			
			

//			System.out.println("**************************Tags***************************");
//			System.out.println(tags[0]);
			} catch (FileNotFoundException ex) {
			   System.out.println("File not found !");
			}
		
		
	} catch (JSONException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	 context.write(new Text(key.toString()), value);
 }

 
 protected void cleanup(Context context)throws IOException, InterruptedException{
 	
 }
}
	
	public static class RestInfo implements WritableComparable{
		
		private Text review_id;
		private Text business_id;
		private IntWritable pos_review_count;
		private IntWritable neg_review_count;
//		private IntWritable stars;
//		private IntWritable votes;

		@Override
		public void readFields(DataInput arg0) throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void write(DataOutput arg0) throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public int compareTo(Object o) {
			// TODO Auto-generated method stub
			return 0;
		}
		
		
	}


public static class IntSumReducer
    extends Reducer<Text,Text,Text,IntWritable> {
 

 public void reduce(Text key, Iterable<IntWritable> values,
                    Context context
                    ) throws IOException, InterruptedException {
 }
}

  public static void main(String[] args) throws Exception {
	  
	  File out = new File(args[1]);
	  if(out.exists()){
		  FileUtils.deleteDirectory(out);
	  }
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(task1.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

