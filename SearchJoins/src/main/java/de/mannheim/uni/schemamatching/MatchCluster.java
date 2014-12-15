package de.mannheim.uni.schemamatching;

import java.util.List;

public class MatchCluster<T> {

	private List<T> cluster;
	private double distance;
	
	public List<T> getCluster() {
		return cluster;
	}
	public void setCluster(List<T> cluster) {
		this.cluster = cluster;
	}
	public double getDistance() {
		return distance;
	}
	public void setDistance(double distance) {
		this.distance = distance;
	}
	public double getSimilarity() {
		return 1/distance;
	}
	
	
	
}
