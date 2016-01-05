package MapReduce_Join.Assignment4;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable,Text,Text,JoinGenericWritable>{
		Text joinkey=new Text();
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			System.out.println("In map");
			String[] record=value.toString().split(",");
			joinkey.set(record[1]);
			Table t= new Table(new Text(record[0]),value);
			JoinGenericWritable gw=new JoinGenericWritable(t);
			context.write(joinkey, gw);
			System.out.println("End of map");
		}
}
