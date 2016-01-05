package edu.asu.cse512;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;




import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
public class RangeQuery implements Serializable{
	/**
	 
	 
	 */
	private static final long serialVersionUID = 1L;

	public static void main(String[] args){
	SparkConf conf =new SparkConf();
    JavaSparkContext spark= new JavaSparkContext(conf);
	JavaRDD<String> text1=spark.textFile(args[0]).cache();
	
	JavaRDD<String> text2=spark.textFile(args[1]).cache();
	JavaRDD<Rectangle> result1=text1.map(new Function<String,Rectangle>(){
		private static final long serialVersionUID = 1L;
		public Rectangle call(String s)
				throws Exception {
			 
			List<String> array=Arrays.asList(s.split(","));
	    	
	    	int id=Integer.parseInt(array.get(0));
	    	double x1=Double.parseDouble(array.get(1));
	    	double y1=Double.parseDouble(array.get(2));
	    	
	    	Rectangle rect = new Rectangle(id,x1,y1);

		
			return rect;
		}

	});
	
	JavaRDD<Rectangle> result2=text2.map(new Function<String,Rectangle>(){
		private static final long serialVersionUID = 1L;
		public Rectangle call(String s)
				throws Exception {
	
			List<String> array=Arrays.asList(s.split(","));
	    	double x1=Double.parseDouble(array.get(0));
	    	System.out.println("X1   :  "+x1);
	    	double y1=Double.parseDouble(array.get(1));
	    	System.out.println("y1   :  "+y1);
	    	double x2=Double.parseDouble(array.get(2));
	    	System.out.println("X2   :  "+x2);
	    	double y2=Double.parseDouble(array.get(3));
	    	System.out.println("y2   :  "+y2);
	    	Rectangle rect = new Rectangle(x1,y1,x2,y2);

			
			return rect;
		}

	});
	
	
	//Broadcasting values of input2 to input1
	 final Broadcast<Rectangle> obj= spark.broadcast(result2.first()); 
	final JavaRDD<Rectangle> finalresult= result1.filter(new Function<Rectangle,Boolean>(){
		private static final long serialVersionUID = 1L;
		public Boolean call(Rectangle arg0) throws Exception {
			
			
			//Calling insideRectangle1 function in Rectangle.java
			return obj.value().insideRectangle1(arg0);
			
			
		}
	}); 
		JavaRDD<Rectangle> finalresult1 = finalresult.repartition(1);
		JavaRDD<Integer> ids=finalresult1.mapPartitions(new FlatMapFunction<Iterator<Rectangle>,Integer>(){

			
			
			private static final long serialVersionUID = 1L;
			
			public Iterable<Integer> call(Iterator<Rectangle> workerresult){
				
				List<Integer> finalresult=new ArrayList<Integer>();
				while(workerresult.hasNext()){
					int temp=workerresult.next().getId();
					finalresult.add(temp);
				}
				
				return finalresult;
			}
        });
		
			
			
		ids.saveAsTextFile(args[2]);
		spark.close();
	}
}
