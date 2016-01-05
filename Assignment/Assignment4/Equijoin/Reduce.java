package MapReduce_Join.Assignment4;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text,JoinGenericWritable,Text,Text> {
		List<Table> selected =new ArrayList<Table>();
		public void reduce(Text key,Iterable<JoinGenericWritable> values,Context context) throws IOException, InterruptedException{
			System.out.println("Inside reducer");
			
			for(JoinGenericWritable t:values){
				Writable w=t.get();
				Table table=(Table)w;
				System.out.println(" Table name : "+table.tablename.toString()+" "+" Columns : "+table.columns.toString());
				selected.add(table);
			}
			
			System.out.println(" Table in the key ");
			if(selected.size()>1){
			for(int i=0;i<selected.size()-1;i++){
					if(!selected.get(i).tablename.equals(selected.get(i+1).tablename))
						context.write(selected.get(i).columns,selected.get(i+1).columns);
				
			}
			}
						
			selected.clear();
		}
		
}
