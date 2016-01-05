package edu.asu.cse512;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.operation.union.CascadedPolygonUnion;



public class convexHull implements Serializable {
	
		/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public static void main(String args[]){
		SparkConf conf =new SparkConf();
			JavaSparkContext spark= new JavaSparkContext(conf);
			JavaRDD<String> text=spark.textFile(args[0]).cache();
	    	
	    	
	    	final JavaRDD<Geometry> result=text.map(new Function<String,Geometry>(){	
	    	
	    	/**
				 * 
				 */
				private static final long serialVersionUID = 1L;
			
				
				public Geometry call(String s) throws Exception{
	    			List<String> dataset=new ArrayList<String>();
	    			dataset = Arrays.asList(s.split(","));
	    			double x = Double.parseDouble(dataset.get(0));
	    			double y = Double.parseDouble(dataset.get(1));
	    			String s1 = String.format("POINT (%.9f %.9f)", x, y);
	    			Geometry store = new WKTReader().read(s1);
	    			
	    		  return store;
	    		}
	    		
	    	});
	    	
	        	
	        
	        /*Gather results from the worker and run the algorithm on the worker's result set to get the final result*/
	        JavaRDD<Geometry> repartition=result.repartition(1);
	        JavaRDD<String> resultfrommaster=repartition.mapPartitions(new FlatMapFunction<Iterator<Geometry>,String>(){

				/**
				 * 
				 */
				private static final long serialVersionUID = 1L;
				public Iterable<String> call(Iterator<Geometry> workerresult){
					List<Geometry> finalresult=new ArrayList<Geometry>();
					while(workerresult.hasNext()){
						finalresult.add(workerresult.next());
					}
					
					ArrayList<String> fullfinalresult = new ArrayList<String>();
					
					if (finalresult.size() > 0) {
						
						//Performing COnvexHull function on Geometry(Points)
						Geometry newresult = CascadedPolygonUnion.union(finalresult).convexHull();
						Coordinate[] count = newresult.getCoordinates();
						List<Points> p1 = new ArrayList<Points>();
						for (int i = 0; i < count.length - 1; i++)  {
							
							Points p = new Points(count[i].x,count[i].y);
							p1.add(p);
						}
						
						//Sorting the Points
						
						Collections.sort(p1,new Comparator<Points>() {

							public int compare(Points o1, Points o2) {
							    return Double.compare(o1.getX(), o2.getX());
							}
							});
						Integer temp1 = p1.size();
						for (int i = 0; i < temp1 ; i++)  {
							String temp = String.format("%.9f,%.9f",p1.get(i).getX(),p1.get(i).getY()); 
							if(!fullfinalresult.contains(temp)) {
								fullfinalresult.add(temp);
							}
							
						
						}
						
					}
					
					
					return  fullfinalresult;
							
				}
	        	
	        });
	        
	        /* Write the result to the output file*/
	        resultfrommaster.saveAsTextFile(args[1]);
	        
			spark.close();

		}
}
