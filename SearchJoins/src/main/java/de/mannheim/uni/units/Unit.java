package de.mannheim.uni.units;

import java.util.List;

/**
 * @author petar
 *
 */
public class Unit
	implements java.io.Serializable
{
	private String name;

	private List<SubUnit> subunits;

	private SubUnit mainUnit;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<SubUnit> getSubunits() {
		return subunits;
	}

	public void setSubunits(List<SubUnit> subunits) {
		this.subunits = subunits;
	}

	public SubUnit getMainUnit() {
		return mainUnit;
	}

	public void setMainUnit(SubUnit mainUnit) {
		this.mainUnit = mainUnit;
	}

	public Unit() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
