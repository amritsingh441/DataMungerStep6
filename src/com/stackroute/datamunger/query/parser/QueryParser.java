package com.stackroute.datamunger.query.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


public class QueryParser {

	
	private QueryParameter queryParameter = new QueryParameter();

	
    /*
	 * This method will parse the queryString and will return the object of
	 * QueryParameter class
	 */
	@SuppressWarnings("null")
	public QueryParameter parseQuery(String queryString) {
		
/*
		 * extract the name of the file from the query. File name can be found after the
		 * "from" clause.
		 */

		/*
		 * extract the order by fields from the query string. Please note that we will
		 * need to extract the field(s) after "order by" clause in the query, if at all
		 * the order by clause exists. For eg: select city,winner,team1,team2 from
		 * data/ipl.csv order by city from the query mentioned above, we need to extract
		 * "city". Please note that we can have more than one order by fields.
		 */

		/*
		 * extract the group by fields from the query string. Please note that we will
		 * need to extract the field(s) after "group by" clause in the query, if at all
		 * the group by clause exists. For eg: select city,max(win_by_runs) from
		 * data/ipl.csv group by city from the query mentioned above, we need to extract
		 * "city". Please note that we can have more than one group by fields.
		 */

		 /*
		 * extract the selected fields from the query string. Please note that we will
		 * need to extract the field(s) after "select" clause followed by a space from
		 * the query string. For eg: select city,win_by_runs from data/ipl.csv from the
		 * query mentioned above, we need to extract "city" and "win_by_runs". Please
		 * note that we might have a field containing name "from_date" or "from_hrs".
		 * Hence, consider this while parsing.
		 */

		/*
		 * extract the conditions from the query string(if exists). for each condition,
		 * we need to capture the following: 1. Name of field 2. condition 3. value
		 * 
		 * For eg: select city,winner,team1,team2,player_of_match from data/ipl.csv
		 * where season >= 2008 or toss_decision != bat
		 * 
		 * here, for the first condition, "season>=2008" we need to capture: 1. Name of
		 * field: season 2. condition: >= 3. value: 2008
		 * 
		 * the query might contain multiple conditions separated by OR/AND operators.
		 * Please consider this while parsing the conditions.
		 * 
		 */

		 /*
		 * extract the logical operators(AND/OR) from the query, if at all it is
		 * present. For eg: select city,winner,team1,team2,player_of_match from
		 * data/ipl.csv where season >= 2008 or toss_decision != bat and city =
		 * bangalore
		 * 
		 * the query mentioned above in the example should return a List of Strings
		 * containing [or,and]
		 */

		/*
		 * extract the aggregate functions from the query. The presence of the aggregate
		 * functions can determined if we have either "min" or "max" or "sum" or "count"
		 * or "avg" followed by opening braces"(" after "select" clause in the query
		 * string. in case it is present, then we will have to extract the same. For
		 * each aggregate functions, we need to know the following: 1. type of aggregate
		 * function(min/max/count/sum/avg) 2. field on which the aggregate function is
		 * being applied
		 * 
		 * Please note that more than one aggregate function can be present in a query
		 * 
		 * 
		 */

		String file = null;
		List<Restriction> restrictions = new ArrayList<Restriction>();
		List<String> logicalOperators = new ArrayList<String>();
		List<String> fields = new ArrayList<String>();;
		List<AggregateFunction> aggregateFunction = new ArrayList<AggregateFunction>();
		List<String> groupByFields = new ArrayList<String>();;
		List<String> orderByFields = new ArrayList<String>();;
		String baseQuery=null;
	
		file = getFile(queryString);

		
		String[] conditions = getConditions(queryString);
		if(conditions!=null) {
		Restriction[] restriction = new Restriction[conditions.length];
		
		for (int i = 0; i < conditions.length; i++) {
			restriction[i] = new Restriction();
			
			String operator=null;
			String value=null;
			String property=null;
			 if(conditions[i].contains("<=")) {
				String[] split = conditions[i].split("<=");
				operator="<=";
				 value=split[1].trim();
				 property=split[0].trim();
				
			}
			else if(conditions[i].contains(">=")) {
				String[] split = conditions[i].split(">=");
				operator=">=";
				 value=split[1].trim();
				 property=split[0].trim();
				
			}
			else if(conditions[i].contains(">")) {
				String[] split = conditions[i].split(">");
				operator=">";
				 value=split[1].trim();
				 property=split[0].trim();
			}
			else if(conditions[i].contains("!=")) {
				String[] split = conditions[i].split("!=");
				operator="!=";
				 value=split[1].trim();
				 property=split[0].trim();
			}
			else if(conditions[i].contains("=")) {
				String[] split = conditions[i].split("=");
				operator="=";
				 value=split[1].trim();
				 property=split[0].trim();
				 if(value.contains("'")) {
					 value= value.replaceAll("'","").trim();
				 }
				
			}
			else if(conditions[i].contains("<")) {
				String[] split = conditions[i].split("<");
				operator="<";
				 value=split[1].trim();
				 property=split[0].trim();
			}
			 
			
			restriction[i].setCondition(operator);
			restriction[i].setPropertyName(property);
			restriction[i].setPropertyValue(value);
			restrictions.add(restriction[i]);

		}
		}
	
		String[] operators = getLogicalOperators(queryString);
		if(operators!=null) {
		for (String op : operators) {
			logicalOperators.add(op);
		}
		}
		
		String[] filds = getFields(queryString);
		if(filds!=null) {
		for (String field : filds) {
			fields.add(field);
		}
		}
		
		String[] aggregationVal = getAggregateFunctions(queryString);
		if(aggregationVal!=null) {
		AggregateFunction[] aggregation = new AggregateFunction[aggregationVal.length];
		for (int i = 0; i < aggregationVal.length; i++) {
			aggregation[i] = new AggregateFunction();
			String[] split = (aggregationVal[i].replace("(", " ")).split(" ");
			System.out.println(split[0]);
			System.out.println(split[1].replace(")", "").trim());
			
			aggregation[i].setFunction(split[0]);
			aggregation[i].setField(split[1].replace(")", "").trim());
			aggregateFunction.add(aggregation[i]);

		}
		}
		
			
		
		String[] groupBy = getGroupByFields(queryString);
		if(groupBy!=null) {
		for (String group : groupBy) {
			groupByFields.add(group);
		}
		}
	
		String[] orderBy = getOrderByFields(queryString);
		if(orderBy!=null) {
		for (String order : orderBy) {
			orderByFields.add(order);
		}
		}
		queryParameter.setFile(file);
		if(restrictions.size()!=0) {
			queryParameter.setRestrictions(restrictions);
		}
		else {
			queryParameter.setRestrictions(null);
		}
		if(logicalOperators.size()!=0) {
		queryParameter.setLogicalOperators(logicalOperators);
		}
		else {
			queryParameter.setLogicalOperators(null);
		}
		baseQuery=getBaseQuery(queryString);
		
		queryParameter.setFields(fields);
		queryParameter.setAggregateFunctions(aggregateFunction);
		queryParameter.setGroupByFields(groupByFields);
		queryParameter.setOrderByFields(orderByFields);
		queryParameter.setBaseQuery(baseQuery.trim());
		return queryParameter;

	}

	
	public static String[] getSplitStrings(String queryString) {
		
		String[] split = queryString.split("\\s+");
		System.out.println("query strings are:");
		for (String s : split) {
			System.out.println(s);
		}
		return split;

	}

	
	public static String getFile(String queryString) {
		
		String file = null;
		String[] split = queryString.split("\\s+");
		for (String s : split) {
			if (s.endsWith("csv")) {
				file = s;
			}
		}
		System.out.println("file is:" + file);
		return file;

	}

	public static String getBaseQuery(String queryString) {
	
		String file = null;
		String baseQuery = null;
		String[] split = null;
		if (queryString.contains(" where ")) {
			split = queryString.split("where");
			baseQuery = split[0];

		} else if (queryString.contains("group by")) {
			split = queryString.split("group by");
			baseQuery = split[0];
		} else if (queryString.contains("order by")) {
			split = queryString.split("order by");
			baseQuery = split[0];
		} else {
			baseQuery = queryString;
		}
		System.out.println("base query:" + baseQuery);
		return baseQuery;
	}

	
	public static String getConditionsPartQuery(String queryString) {
		
		if (queryString.contains(" where ")) {
			String[] split = queryString.split("where");
			String conditionalPart = split[1];

			if (split[1].contains("group")) {
				conditionalPart = (split[1].split("group by"))[0];
			}
			if (split[1].contains("order")) {
				conditionalPart = (split[1].split("order by"))[0];
			}
			System.out.println("getConditionsPartQuery:" + split[1]);
			return conditionalPart;
		} else {
			return null;
		}
	}

	public static String[] getConditions(String queryString) {

		//queryString = queryString.toLowerCase();
		String condtionalPart = getConditionsPartQuery(queryString);

		String[] conditions = null;

		if (condtionalPart != null) {

			if (condtionalPart.contains(" and ") && condtionalPart.contains(" or ")) {
				List<String> list = new ArrayList<String>();
				if (condtionalPart.indexOf(" and ") < condtionalPart.indexOf(" or ")) {
					list.add(condtionalPart.split(" and ")[0]);
					list.add((condtionalPart.split(" and ")[1]).split(" or ")[0]);
					list.add((condtionalPart.split(" and ")[1]).split(" or ")[1]);
				} else {
					list.add(condtionalPart.split(" or ")[0]);
					list.add((condtionalPart.split(" or ")[1]).split(" and ")[0]);
					list.add((condtionalPart.split(" or ")[1]).split(" and ")[1]);
				}
				conditions = new String[list.size()];
				for (int i = 0; i < list.size(); i++) {
					conditions[i] = list.get(i).trim();
				}
			} else if (condtionalPart.contains(" and ")) {
				conditions = condtionalPart.split(" and ");
				
			}

			else if (condtionalPart.contains(" or ")) {
				conditions = condtionalPart.split(" or ");
			
			}

			else {
				conditions = new String[1];
				conditions[0] = condtionalPart;
				
			}

			for (int i = 0; i < conditions.length; i++) {
				conditions[i] = conditions[i].trim();
			}
			System.out.println("conditions are:" + Arrays.toString(conditions));
			return conditions;

		} else {
			return null;
		}
	}

	public static String[] getLogicalOperators(String queryString) {
		
		List<String> logicalOp = new ArrayList<String>();
		if (queryString.contains(" or ")) {
			logicalOp.add("or");
		}
		if (queryString.contains(" OR ")) {
			logicalOp.add("OR");
		}
		if (queryString.contains(" AND ")) {
			logicalOp.add("AND");
		}
		if (queryString.contains(" and ")) {
			logicalOp.add("and");
		}
		String[] logicalOperators = new String[logicalOp.size()];

		if (queryString.contains(" or ") && queryString.contains(" and ")) {
			if (queryString.indexOf(" and ") < queryString.indexOf(" or ")) {
				Collections.reverse(logicalOp);
			}

		}
		for (int i = 0; i < logicalOp.size(); i++) {
			logicalOperators[i] = logicalOp.get(i);
		}
		System.out.println("logical operators are:" + Arrays.toString(logicalOperators));
		if (logicalOperators.length != 0) {
			return logicalOperators;

		} else {
			return null;
		}
	}

	
	public static String[] getFields(String queryString) {
		
		String baseQuery = getBaseQuery(queryString);

		if (((baseQuery.split("select|from"))[1]).contains(",")) {
			String[] baseFields = ((baseQuery.split("select|from")[1])).split(",");

			System.out.println("Base fields:" + Arrays.toString(baseFields));
			for(int i=0;i<baseFields.length;i++){
				
			
					baseFields[i]=baseFields[i].trim();
				
			}
			return baseFields;
		} else {
			String[] baseFields = new String[1];
			
			baseFields[0] = ((baseQuery.split("select|from"))[1]).trim();
			System.out.println("Base fields:" + Arrays.toString(baseFields));
			return baseFields;
		}
	}

	
	public static String[] getOrderByFields(String queryString) {
		//queryString = queryString.toLowerCase();
		if (queryString.contains("order by")) {
			String[] orderBy = { (queryString.split("order by"))[1] };
			for (int i = 0; i < orderBy.length; i++) {
				orderBy[i] = orderBy[i].trim();
			}
			return orderBy;
		} else {
			return null;
		}

	}

	
	public static String[] getGroupByFields(String queryString) {
		
		if (queryString.contains("group by")) {
			String[] groupBy = { (queryString.split("group by"))[1] 
					
			
			};
			
			if(groupBy[0].contains("order by")){
				groupBy[0] = (groupBy[0].split("order by"))[0] ;	
			}
			for (int i = 0; i < groupBy.length; i++) {
				groupBy[i] = groupBy[i].trim();
			}
			System.out.println("group by fields are:" + Arrays.toString(groupBy));
			return groupBy;
		} else {
			return null;
		}

	}

	public static String[] getAggregateFunctions(String queryString) {
		
		String baseQuery = getBaseQuery(queryString);
		if (((baseQuery.split("\\s+"))[1]).contains("sum") || ((baseQuery.split("\\s+"))[1]).contains("max")
				|| ((baseQuery.split("\\s+"))[1]).contains("min") || ((baseQuery.split("\\s+"))[1]).contains("count") || ((baseQuery.split("\\s+"))[1]).contains("avg")) {
			String[] agregateFields = ((baseQuery.split("\\s+"))[1]).split(",");
			List<String> agFunctions=new ArrayList<>();
			for(String a:agregateFields) {
				if (a.contains("sum") || a.contains("max")
						|| a.contains("min") || a.contains("count") || a.contains("avg")) {
					agFunctions.add(a);
			
				}
		}
			String[] filteredAg=new String[agFunctions.size()];
			for(int i=0;i<agFunctions.size();i++) {
				filteredAg[i]=agFunctions.get(i);
			}
			System.out.println("Aggregate functions:" + Arrays.toString(filteredAg));
			return filteredAg;
		}else {
			return null;
		}
	}
	
}