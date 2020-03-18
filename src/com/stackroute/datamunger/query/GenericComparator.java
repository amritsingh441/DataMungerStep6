package com.stackroute.datamunger.query;

import java.util.Comparator;

/*
 * The GenericComparator class implements Comparator Interface. This class is used to 
 * compare row objects which will be used for sorting the dataSet
 */
public class GenericComparator implements Comparator<Row> {

	String column=null;
	String dataType=null;
	int order=0;
	public GenericComparator(String column,String dataType,int order){
		this.column=column;
		this.dataType=dataType;
		this.order=order;
	}
	@Override
	public int compare(Row o1, Row o2) {
		if(dataType.equals("java.lang.String")) {
			return order*(o1.get(column).compareTo(o2.get(column)));
		}
		else  {
			return order*(Integer.compare(Integer.valueOf(o1.get(column)),Integer.valueOf(o2.get(column))));
		}
	}

}
