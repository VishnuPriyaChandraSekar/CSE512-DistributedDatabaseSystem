package edu.asu.cse512;



import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import math.geom2d.Point2D;
import math.geom2d.polygon.Polygon2D;
import math.geom2d.polygon.Polygons2D;
import math.geom2d.polygon.SimplePolygon2D;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;











public class Union implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public static void main(String[] args){
		SparkConf conf =new SparkConf();
		/* Setting up configuration for Spark */
		
		JavaSparkContext spark= new JavaSparkContext(conf);
    	JavaRDD<String> text=spark.textFile(args[0]).cache();
    	JavaRDD<Polygon> resultfrommaster=text.map(new Function<String, Polygon>(){

    		private static final long serialVersionUID = 1L;
    		
				
				public Polygon call(String s) throws Exception {
					String[] stringstore = s.split(",");
					double x1 = Math.min(Double.parseDouble(stringstore[0]), Double.parseDouble(stringstore[2]));
					double x2 = Math.max(Double.parseDouble(stringstore[0]), Double.parseDouble(stringstore[2]));
					double y1 = Math.min(Double.parseDouble(stringstore[1]), Double.parseDouble(stringstore[3]));
					double y2 = Math.max(Double.parseDouble(stringstore[1]), Double.parseDouble(stringstore[3]));
					
					Polygon poly = new Polygon();
					poly.addPoint(x1,y1);
					poly.addPoint(x1,y2);
					poly.addPoint(x2,y2);
					poly.addPoint(x2,y1);
					return poly;
				
				
				
			} 				
    	
	});
    	Polygon poly = resultfrommaster.reduce(new Function2<Polygon,Polygon,Polygon>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Polygon call(Polygon p1, Polygon p2) throws Exception {
				
				Polygon2D polystore1 = new SimplePolygon2D();
				Polygon2D polystore2 = new SimplePolygon2D();
				
				for(Points point : p1.points){
					polystore1.addVertex(new Point2D(point.getX(), point.getY()));
				}
				
				for(Points point : p2.points){
					polystore2.addVertex(new Point2D(point.getX(), point.getY()));
				}
				
				//Performing Union function on multiple polygons
				
				Polygon2D polyunion = (Polygon2D) Polygons2D.union(polystore1, polystore2);
				Collection<Point2D> points = polyunion.vertices();
				Polygon polyfinal = new Polygon();
				for(Point2D point : points){
					polyfinal.addPoint(new Points(point.getX(), point.getY()));
				}
				return polyfinal;
				
				
			}
    		
    		
    		
    	});
    	
    	//Sorting the Points in the Polygon
    	List<Points> p1 = new ArrayList<Points>();
    	ArrayList<String> fullfinalresult = new ArrayList<String>();
    	for(Points point : poly.points){
    		Points p = new Points(point.getX(),point.getY());
    		
    		p1.add(p);
			
		}
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
    	JavaRDD<String> completeresult = spark.parallelize(fullfinalresult);
    	JavaRDD<String> completeresult1 = completeresult.repartition(1);
    	 completeresult1.saveAsTextFile(args[1]);
 		spark.close();

	}
}