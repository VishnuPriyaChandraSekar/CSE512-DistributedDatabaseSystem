package edu.asu.cse512;

import java.io.Serializable;
import java.util.Comparator;

public class Points implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private double x;
	private double y;
	public Points(double x,double y){
		this.x=x;
		this.y=y;
	}
	public double getX() {
		return x;
	}
	public void setX(int x) {
		this.x = x;
	}
	public double getY() {
		return y;
	}
	public void setY(int y) {
		this.y = y;
	}
	public static Comparator<Points> SortByX=new Comparator<Points>(){
		public int compare(Points p,Points q){
			
			return Double.compare(p.getX(), q.getX());
		}
		
	};
	
	public static Comparator<Points> SortByY= new Comparator<Points>(){
		public int compare(Points p,Points q){
			return Double.compare(p.getY(),q.getY());
		}
	};

	
}
