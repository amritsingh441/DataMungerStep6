package com.stackroute.datamunger.query;

import java.util.Collections;
import java.util.List;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import com.stackroute.datamunger.query.parser.Restriction;

//This class contains methods to evaluate expressions
public class Filter {

	/*
	 * the evaluateExpression() method of this class is responsible for evaluating
	 * the expressions mentioned in the query. It has to be noted that the process
	 * of evaluating expressions will be different for different data types. there
	 * are 6 operators that can exist within a query i.e. >=,<=,<,>,!=,= This method
	 * should be able to evaluate all of them. Note: while evaluating string
	 * expressions, please handle uppercase and lowercase
	 * 
	 */

	// method to evaluate expression for eg: salary>20000

	// method containing implementation of equalTo operator

	// method containing implementation of greaterThan operator

	// method containing implementation of greaterThanOrEqualTo operator

	// method containing implementation of lessThan operator

	// method containing implementation of lessThanOrEqualTo operator
	public static Boolean evaluateExpression (List<Restriction> restriction,List<String> logicalOperators,String[] row,Header header,RowDataTypeDefinitions rowDataType) {
	String[] logicalOpratorList=null;
	
	if(logicalOperators!=null) {
		if((logicalOperators.size()>1 ) && (logicalOperators.get(logicalOperators.size()-1)).equals("and") ) {
			Collections.reverse(logicalOperators);
			Collections.reverse(restriction);
			
		}
		logicalOpratorList=new String[logicalOperators.size()];
	
		
	for(int i=0;i<logicalOperators.size();i++) {
		if(logicalOperators.get(i).equals("and")) {
			logicalOpratorList[i]="&&";
		}
		else {
			logicalOpratorList[i]="||";
		}
	}
	
	}
	String expressionStr=null;
	for(int i=0;i<restriction.size();i++) {
		String property=restriction.get(i).getPropertyName();
		String op=restriction.get(i).getCondition();
		if(op=="=") {
			op="==";
		}
		String value=restriction.get(i).getPropertyValue();
		String propVal=row[(header.get(property))-1];
	Boolean isString=rowDataType.get((header.get(property))).equals("java.lang.String");
		if(i==0) {
			if(isString) {
			expressionStr= "(\""+propVal.toLowerCase()+"\""+" "+op+" "+"\""+value.toLowerCase()+"\")";
			}
			else {
				expressionStr= "("+propVal+" "+op+" "+value+")";
			}
		}
		else  {
			String oprtrVal=logicalOpratorList[i-1];
			if(isString) {
			expressionStr=expressionStr+" "+oprtrVal+" "+"(\""+propVal.toLowerCase()+"\""+" "+op+" "+"\""+value.toLowerCase()+"\")";
			
			}
			else {
				expressionStr=expressionStr+" "+oprtrVal+" ("+propVal+" "+op+" "+value+")";
			}
			
			}
		if(((i+1)%2)==0) {
			expressionStr="("+expressionStr+")";
		}
		
	}
	ScriptEngineManager scriptEngnMngr = new ScriptEngineManager();
    ScriptEngine scriptEngine = scriptEngnMngr.getEngineByName("JavaScript");
    try {
    	return (Boolean) scriptEngine.eval(expressionStr);
	} catch (ScriptException e) {
		e.printStackTrace();
		return false;
	}

		
}
}
