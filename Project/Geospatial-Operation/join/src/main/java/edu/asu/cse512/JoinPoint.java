package edu.asu.cse512 ;






import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;




import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

public class JoinPoint {
	
	
	
public static void main(String args[])
{
	SparkConf conf =new SparkConf();
	JavaSparkContext spark= new JavaSparkContext(conf);
	//JavaRDD<String> text1=spark.textFile(args[0]).cache();
	
//	JavaRDD<String> text2=spark.textFile(args[1]).cache();
	
JavaRDD<String> text1=spark.textFile(args[0]).cache();
	
	JavaRDD<String> text2=spark.textFile(args[1]).cache();
	JavaRDD<Rectangle> result1=text1.map(new Function<String,Rectangle>(){
		private static final long serialVersionUID = 1L;
		public Rectangle call(String s)
				throws Exception {
			 
			List<String> array=Arrays.asList(s.split(","));
	    	//List<String> store=new ArrayList<String>();
	    	//System.out.println("works1");
	    	int id=Integer.parseInt(array.get(0));
	    	double x1=Double.parseDouble(array.get(1));
	    	double y1=Double.parseDouble(array.get(2));
	    	//double x2=Double.parseDouble(array.get(3));
	    	//double y2=Double.parseDouble(array.get(4));
	    	
	    	Rectangle rect = new Rectangle(id,x1,y1);

			// TODO Auto-generated method stub
			return rect;
		}
	});
	
	JavaRDD<Rectangle> result2=text2.map(new Function<String,Rectangle>(){
		private static final long serialVersionUID = 1L;
		public Rectangle call(String s)
				throws Exception {
	
			List<String> array=Arrays.asList(s.split(","));
	    	int id1=Integer.parseInt(array.get(0));
	    	double x1=Double.parseDouble(array.get(1));
	    	double y1=Double.parseDouble(array.get(2));
	    	double x2=Double.parseDouble(array.get(3));
	    	double y2=Double.parseDouble(array.get(4));
	    	
	    	Rectangle rect = new Rectangle(id1,x1,y1,x2,y2);

			// TODO Auto-generated method stub
			return rect;
		}

		

		
		
		
	});
	
	 final Broadcast<List<Rectangle>> obj= spark.broadcast(result1.collect()); 
	final JavaRDD<HashMap<Integer,ArrayList<Integer>>> finalresult= result2.map(new Function<Rectangle,HashMap<Integer,ArrayList<Integer>>>(){
		private static final long serialVersionUID = 1L;

		public HashMap<Integer, ArrayList<Integer>> call(Rectangle rect)
				throws Exception {
			HashMap<Integer,ArrayList<Integer>> newstore = new HashMap<Integer,ArrayList<Integer>>();
			List<Rectangle> store =  obj.value();
			ArrayList<Integer> store1 = new ArrayList<Integer>();
			int len = store.size();
			Integer res = rect.getId();
			
			for(int i=0;i<len;i++){
				
				if(store.get(i).insideRectangle1(rect)){
					store1.add(store.get(i).getId());

				}
				
				
			}
			newstore.put(res,store1);
			//store1.add(99);
			
			
			// TODO Auto-generated method stub
			return newstore;
		}
		
	}); 
		
		
	JavaRDD<HashMap<Integer,ArrayList<Integer>>> finalresult1 = finalresult.repartition(1);
	//JavaRDD<Integer> totalresult = 
	//finalresult1.get(0);
	JavaRDD<String> resultfrommaster=finalresult1.mapPartitions(new FlatMapFunction<Iterator<HashMap<Integer,ArrayList<Integer>>>,String>(){

		
		private static final long serialVersionUID = 1L;

		public Iterable<String> call(Iterator<HashMap<Integer, ArrayList<Integer>>> hm)
				throws Exception {
			// TODO Auto-generated method stub
			ArrayList<String> correctformat = new ArrayList<String>();
			while(hm.hasNext()){
				HashMap<Integer, ArrayList<Integer>> map = new HashMap<Integer, ArrayList<Integer>>();
				map = hm.next();
			int outerlength = map.size(); 
			
			for ( Entry<Integer, ArrayList<Integer>> entry : map.entrySet()) {
			   
				String temp="";
				temp = temp+String.format("%d",entry.getKey());
				
				ArrayList<Integer> values = entry.getValue();
				
				int size = values.size();
				for(int i=0;i<size;i++){
					
					temp = temp+","+String.format("%d", values.get(i));
				}
				
				
				correctformat.add(temp);
				
			}
			
			
			}
			return correctformat;
		}

    });
	JavaRDD<String> totalresult = resultfrommaster.repartition(1);
	

		
		//totalresult.saveAsTextFile(args[2]);
		totalresult.saveAsTextFile(args[2]);
		spark.close();
			
}

	
}