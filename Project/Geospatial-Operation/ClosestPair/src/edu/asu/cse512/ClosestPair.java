package edu.asu.cse512;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;



public class ClosestPair implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public static void main(String[] args){
/* Setting up configuration for Spark */
		
		SparkConf conf =new SparkConf();
    	JavaSparkContext spark= new JavaSparkContext(conf);
    	JavaRDD<String> text=spark.textFile(args[0]).cache();
    	JavaRDD<Points> result=text.mapPartitions(new FlatMapFunction<Iterator<String>,Points>(){
    		/**
			 * 
			 */
			private static final long serialVersionUID = 1L;
		
			/*Extract the input from file and send them to the workers*/
			public Iterable<Points> call(Iterator<String> lines){
    			List<Points> dataset=new ArrayList<Points>();
    			while(lines.hasNext()){
    				String line=lines.next();
    				String[] word=line.split(",");
    						Points point = new Points(Double.valueOf((word[0].trim())),Double.valueOf((word[1].trim())));
    						dataset.add(point);
    			}
    		  return MainDivideAndConquer(dataset);
    		}
    		
    	});
	
        
        /*Gather results from the worker and run the algorithm run on the worker's result set to get the final result*/
        JavaRDD<Points> repartition=result.repartition(1);
        JavaRDD<String> resultfrommaster=repartition.mapPartitions(new FlatMapFunction<Iterator<Points>,String>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;
			public Iterable<String> call(Iterator<Points> workerresult){
				List<Points> finalresult=new ArrayList<Points>();
				while(workerresult.hasNext()){
					Points p=workerresult.next();
					finalresult.add(p);
				}
				List<Points> answer=MainDivideAndConquer(finalresult);
				System.out.println("The closest pair : ("+answer.get(0).getX()+" , "+answer.get(0).getY()+")");
				System.out.print(" and  ("+answer.get(1).getX()+" , "+answer.get(1).getY()+")");
				
				List<String> result=new ArrayList<String>();
				result.add(answer.get(0).getX()+","+answer.get(0).getY());
				result.add(answer.get(1).getX()+","+answer.get(1).getY());
				return  result;
						
			}
        	
        });
        
        /* Write the result to the output file*/
        resultfrommaster.saveAsTextFile(args[1]);
		spark.close();

	}
	public static List<Points> MainDivideAndConquer(List<Points> dataset){
		List<Points> xSort=new ArrayList<Points>(dataset);
		SortCoordinates(xSort,"x");
		List<Points> ySort=new ArrayList<Points>(dataset);
		SortCoordinates(ySort,"y");
		List<Points> closest=DivideAndConquer(xSort,ySort);
		return closest;
	}
	
	public static List<Points> BruteForce(List<Points> Points){
		double distance,min=Double.MAX_VALUE;
		List<Points> minimum=new ArrayList<Points>();
		int index1=0,index2=0;
		if(Points.size()< 2)
			return null;
		else{
			for(int i=0;i<Points.size()-1;i++){
				for(int j=i+1;j<Points.size();j++){
					distance=ComputeDistance(Points.get(i),Points.get(j));
					if(distance<min){
						min=distance;
						index1=i;
						index2=j;
						}
					
					}
				}
			minimum.add(Points.get(index1));
			minimum.add(Points.get(index2));
			return minimum;
		}
	}
    
	public static  double ComputeDistance(Points p , Points q){
		
		double x=Math.pow(p.getX()-q.getX(),2);
		double y=Math.pow(p.getY()-q.getY(),2);
		return Math.sqrt(x+y);
	
	}
	
	
	public static List<Points> DivideAndConquer(List<Points> xPoints,List<Points> yPoints){
    	
		int median ;
    	double delta,d1=0,d2=0;
    	if( xPoints.size() <  4 )
    		return BruteForce(xPoints);
    	else{
    		median = xPoints.size() / 2;
    		List<Points> xRight=xPoints.subList(0,median);
    		List<Points> xLeft=xPoints.subList(median,xPoints.size());
    		List<Points> tmp= new ArrayList<Points>(xRight);
    		List<Points> right=new ArrayList<Points>(DivideAndConquer(xRight,SortCoordinates(tmp,"y")));
    		tmp.clear();
    		tmp.addAll(xLeft);
    		List<Points> left=new ArrayList<Points>(DivideAndConquer(xLeft,SortCoordinates(tmp,"y")));
    		
    		if(right!=null)
    			d1= ComputeDistance(right.get(0),right.get(1));
    		if(left!=null)
    			d2=ComputeDistance(left.get(0),left.get(1));
    		
    		delta=Math.min(d1,d2); 		
    		
    		List<Points> ylist=FindListWithinMedian(yPoints,delta);
    		
    		if(ylist.size()<=1)
    			return d1 < d2 ? right : left;
    		else{
    			 List<Points> ptmedian=BruteForce(ylist);
    			 if(ComputeDistance(ptmedian.get(0),ptmedian.get(1))<delta)
    				 return ptmedian;
    			 else
    				 return d1 < d2 ? right : left;
    		}
    			
    	}
    }
    
	public static List<Points> FindListWithinMedian(List<Points> yPoints,double delta){
		Iterator<Points> iter=yPoints.iterator();
        while(iter.hasNext()){
        	Points p=iter.next();
        	if(p.getX() > delta )
        		iter.remove();
		}
		return yPoints;
	}
       
    public static List<Points> SortCoordinates(List<Points> points,String coordinates){
    	if(coordinates.equals("y"))
    		Collections.sort(points,Points.SortByY);
    	else
    		Collections.sort(points,Points.SortByX);
    	return points;
	}


}
