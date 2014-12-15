package de.mannheim.uni.utils.concurrent;

public abstract class Task {
	private Object userData;
	public Object getUserData() {
		return userData;
	}
	public void setUserData(Object userData) {
		this.userData = userData;
	}
	public abstract void execute();
}
