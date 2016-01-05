package edu.asu.cse512;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.operation.union.CascadedPolygonUnion;



public class FarthestPair implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * 
	 */
	public static void main(String[] args){
	
	SparkConf conf =new SparkConf();
		final ArrayList<String> fullfinalresult = new ArrayList<String>();
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
				
				
				// Performing ConvexHull function on Geometry(Points)
				if (finalresult.size() > 0) {
					Geometry newresult = CascadedPolygonUnion.union(finalresult).convexHull();
					
					Coordinate[] count = newresult.getCoordinates();
					for (int i = 0; i < count.length - 1; i++)  {
						String temp = String.format("%.9f,%.9f",count[i].x,count[i].y); 
						if(!fullfinalresult.contains(temp)) {
							fullfinalresult.add(temp);
						}
						
					
					}
					
				}
				
				
				return  fullfinalresult;
						
			}
        	
        });
        
    	
    	JavaRDD<Points> result2=resultfrommaster.mapPartitions(COORDINATE_MAP);
    	
    	
    	JavaRDD<String> resultfrommaster1=result2.mapPartitions(new FlatMapFunction<Iterator<Points>,String>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;
			public Iterable<String> call(Iterator<Points> workerresult){
				List<Points> finalresult3=new ArrayList<Points>();
				while(workerresult.hasNext()){
					Points p=workerresult.next();
					finalresult3.add(p);
				}
			
				List<String> result=new ArrayList<String>();
				result.add(finalresult3.get(0).getX()+","+finalresult3.get(0).getY());
				result.add(finalresult3.get(1).getX()+","+finalresult3.get(1).getY());
				return  result;
						
			}
        	
        });
//*Gather results from the worker and run the algorithm on the worker's result set to get the final result*/
    	JavaRDD<String> resultfrommaster2 = resultfrommaster1.repartition(1);
    	
    	//Saving result in a text file
    	resultfrommaster2.saveAsTextFile(args[1]);
    	spark.close();
    	
	}
	public static  ArrayList<Points> BruteforceDistance(ArrayList<Points> Points){
		double distance,max=Double.MAX_VALUE;max=0;
		ArrayList<Points> maximum=new ArrayList<Points>();
		int index1=0,index2=0;
		if(Points.size()< 2)
			return null;
		else{
			for(int i=0;i<Points.size()-1;i++){
				for(int j=i+1;j<Points.size();j++){
					distance=ComputeDistance(Points.get(i),Points.get(j));
					if(distance>max){
						max=distance;
						index1=i;
						index2=j;
						}
					
					}
				}
			maximum.add(Points.get(index1));
			maximum.add(Points.get(index2));
			return maximum;
		}
	}
  public static  double ComputeDistance(Points p , Points q){
		
		double x=Math.pow(p.getX()-q.getX(),2);
		double y=Math.pow(p.getY()-q.getY(),2);
		return Math.sqrt(x+y);
	
	}
  private final static FlatMapFunction<Iterator<String>, Points> COORDINATE_MAP =
			new FlatMapFunction<Iterator<String>, Points>() {
		private static final long serialVersionUID = 1L;
		
		public Iterable<Points> call(Iterator<String> lines){
			ArrayList<Points> dataset=new ArrayList<Points>();
			while(lines.hasNext()){
				String line=lines.next();
				String[] word=line.split(",");
						Points point = new Points(Double.valueOf((word[0].trim())),Double.valueOf((word[1].trim())));
						dataset.add(point);
			}
		  return BruteforceDistance(dataset);
		}
	};

}
