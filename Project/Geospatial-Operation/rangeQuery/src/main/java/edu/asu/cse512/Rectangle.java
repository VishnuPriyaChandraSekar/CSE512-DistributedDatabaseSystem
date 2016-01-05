package edu.asu.cse512;

import java.io.Serializable;

public class Rectangle implements Serializable {
	private static final long serialVersionUID = 1L;
	private double x1,x2,y1,y2;
	private int id;

public Rectangle(int id,double x1,double y1,double x2,double y2)
{
	this.id=id;
	this.x1=x1;
	this.y1=y1;
	this.x2=x2;
	this.y2=y2;
}
public Rectangle(int id,double x1,double y1)
{
	this.id=id;
	this.x1=x1;
	this.y1=y1;
}
public Rectangle(double x1,double y1,double x2,double y2)
{
	this.x1=x1;
	this.y1=y1;
	this.x2=x2;
	this.y2=y2;
	this.x1=Math.min(x1,x2);
	this.y1=Math.min(y1, y2);
	this.x2=Math.max(x1,x2);
	this.y2=Math.max(y1, y2);
}
public Boolean insideRectangle(Rectangle rect)
{
	if(rect.x1>=x1&& rect.x1<=x2&&rect.x2>=x1&&rect.x2<=x2&&rect.y1>=y1&&rect.y1<=y2&&rect.y2>=y1&&rect.y2<=y2)
		return true;
	else 
		return false;
}

public Boolean insideRectangle1(Rectangle rect)
{
	if(rect.x1>=x1&&rect.x2>=x1&&rect.y1>=y1&&rect.y2>=y1)
		return true;
	else 
		return false;
}



public int getId() {
	return id;
}
public void setId(int id) {
	this.id = id;
}
public double getX1() {
	return x1;
}

public double getX2() {
	return x2;
}

public double getY1() {
	return y1;
}

public double getY2() {
	return y2;
}
}