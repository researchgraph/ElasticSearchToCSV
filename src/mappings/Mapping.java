package mappings;

import java.util.HashMap;
import java.util.TreeSet;

import json2csv.Merge.Mode;
import tools.Tools;

public abstract class Mapping {
	public HashMap<String, HashMap<String, Integer>> columnIndexMap;
	public HashMap<String, String> headerMap;
	
	public HashMap<String, Integer> relColumnIndexMap;
	public String relationshipHeader;
	
	protected String csvSeparator;
	protected String currentDateAndTime;
	
	
		
	public Mapping(String csvSeparator, String currentDateAndTime, Mode mode){
		this.csvSeparator = csvSeparator;
		this.currentDateAndTime = currentDateAndTime;
		
		if(mode == Mode.EDGES){
			setRelationshipIndicesAndHeaders();
		}
		else{			
			setColumnsIndicesAndHeaders();
		}
	}
	
	public abstract void setRelationshipIndicesAndHeaders();
	public abstract void setColumnsIndicesAndHeaders();
	
//	### indices ###
		
	protected HashMap<String, Integer> defineColumnIndexes(TreeSet<String> columns){
		HashMap<String, Integer> columnsWithIndexes = new HashMap<>();
		
		int index = 0;
		for(String column: columns){
			columnsWithIndexes.put(column, index++);
		}		
		
		return columnsWithIndexes;
	}
	
//	### headers ####
	
	protected String createHeader(TreeSet<String> columns, String entityType){
		String header = Tools.getArrayAsString(columns, csvSeparator);
		return prepareHeader(header, entityType);
	}
	
	protected String prepareHeader(String header, String entityType){	
		if(entityType == null)
			header = doRelationshipHeaderColumnNameMapping(header);
		else
			header = doHeaderColumnNameMapping(header, entityType);
		
		return header;
	}
	
//	### mapping ###
	protected abstract String doHeaderColumnNameMapping(String header, String entityType);
	protected abstract String doRelationshipHeaderColumnNameMapping(String header);	
	
//	### research graph values ###
	public abstract void addResearchGraphValues(String[] orderedElements, HashMap<String, Integer> columnsWithIndexes, String local_id, String currentEntityType);
	public abstract void addRelationshipResearchGraphValues(String[] orderedElements, HashMap<String, Integer> columnsWithIndexes);
	
//	### labels ###	
	public abstract String getLabel(String entityType);
}
