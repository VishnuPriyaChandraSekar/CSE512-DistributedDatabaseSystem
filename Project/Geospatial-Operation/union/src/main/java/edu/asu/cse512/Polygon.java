package edu.asu.cse512;

import java.io.Serializable;
import java.util.ArrayList;

/*
 * User defined class to store polygons
 * as replacement for Polygon2D
 * so as to implement Serializable
 * 
 * */

public class Polygon implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	//public boolean isMulti = false;
	public ArrayList<Points> points = new ArrayList<Points>();
	
	
	public void addPoint(double x,  double y){
		points.add(new Points(x,y));
	}
	
	
	public void addPoint(Points point){
		points.add(point);
	}
	
	public double getMinX(){
		return points.get(0).getX();
	}
	public double getMinY(){
		return points.get(0).getY();
	}
	public double getMaxX(){
		return points.get(2).getX();
	}
	public double getMaxY(){
		return points.get(2).getY();
	}
}