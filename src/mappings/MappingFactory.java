package mappings;

import json2csv.Merge.Mode;

public class MappingFactory {
	String dataSource;
	
	public MappingFactory(String dataSource){
		this.dataSource = dataSource;
	}
	
	public Mapping getMapping(String csvSeparator, String currentDateAndTime, Mode mode){
		System.out.println("datasource: " + this.dataSource);
		if(this.dataSource.equals("GESIS"))
			return getGESIS_Mapping(csvSeparator, currentDateAndTime, mode);
		else if(this.dataSource.equals("NIH"))
			return getNIH_Mapping(csvSeparator, currentDateAndTime, mode);
		
		return null;
	}
	
	public GESIS_Mapping getGESIS_Mapping(String csvSeparator, String currentDateAndTime, Mode mode){
		return new GESIS_Mapping(csvSeparator, currentDateAndTime, mode);
	}
	
	public NIH_Mapping getNIH_Mapping(String csvSeparator, String currentDateAndTime, Mode mode){
		return new NIH_Mapping(csvSeparator, currentDateAndTime, mode);
	}
}
