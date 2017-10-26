package mappings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeSet;

import json2csv.Merge.Mode;

public class NIH_Mapping extends Mapping{
	
	public NIH_Mapping(String csvSeparator, String currentDateAndTime, Mode mode){
		super(csvSeparator, currentDateAndTime, mode);
	}	

	public enum EntityTypes{		
		project,
		publication,
		clinicalStudy,
		entityLink;
	}	
	
	protected String[] getNodeEntitiesNames(){
		return new String[]{
				EntityTypes.project.toString(),
				EntityTypes.publication.toString(),
				EntityTypes.clinicalStudy.toString()
		};		
	}
	
	protected String[] getEdgeEntitiesNames(){
		return new String[]{
				EntityTypes.entityLink.toString()
		};		
	}
	
	protected TreeSet<String> getEntityColumns(String entityName){
		EntityTypes type = EntityTypes.valueOf(entityName);
		
		switch(type){
		case project: return getProjectColumns();
		case publication: return getPublicationColumns();
		case clinicalStudy: return getClinicalStudiesColumns();
		case entityLink: return getLinkTables();
		}
		
		return null;
	}
	
	@Override
	public String getLocalIDFieldName(String currentEntityType){
		EntityTypes type = EntityTypes.valueOf(currentEntityType);
		switch(type){
		case project: return "CORE_PROJECT_NUM";
		case clinicalStudy: return "ClinicalTrials.gov ID";
		case publication: return "PMID";
		case entityLink: return "";
		}
		
		return localIDFieldName;
	}
	
	public String getCurrentEntityType(String[] columnNames, ArrayList<String> rowElements){
		for(String columnName: columnNames){
			if(columnName.equals("CORE_PROJECT_NUM"))
				return EntityTypes.project.toString();
			if(columnName.equals("ClinicalTrials.gov ID"))
				return EntityTypes.clinicalStudy.toString();
			if(columnName.equals("PMID"))
				return EntityTypes.publication.toString();
		}
		
		return null;
	}
	
	
//	### headers and mappings #######################################
	
	private TreeSet<String> getPublicationColumns(){
		TreeSet<String> columns = new TreeSet<>();
		columns.add("AFFILIATION");
		columns.add("AUTHOR_LIST");
		columns.add("COUNTRY");
		columns.add("ISSN");
		columns.add("JOURNAL_ISSUE");
		columns.add("JOURNAL_TITLE");
		columns.add("JOURNAL_TITLE_ABBR");
		columns.add("JOURNAL_VOLUME");
		columns.add("LANG");
		columns.add("PAGE_NUMBER");
		columns.add("PMC_ID");
		columns.add("PMID");
		columns.add("PUB_DATE");
		columns.add("PUB_TITLE");
		columns.add("PUB_YEAR");

//		Research Graph schema
		addResearchGraphSchema(columns);
		columns.add("doi");
		
		return columns;
	}
	
	private TreeSet<String> getProjectColumns(){
		TreeSet<String> columns = new TreeSet<>();
		columns.add("APPLICATION_ID");
		columns.add("ACTIVITY");
		columns.add("ADMINISTERING_IC");
		columns.add("APPLICATION_TYPE");
		columns.add("ARRA_FUNDED");
		columns.add("AWARD_NOTICE_DATE");
		columns.add("BUDGET_START");
		columns.add("BUDGET_END");
		columns.add("CFDA_CODE");
		columns.add("CORE_PROJECT_NUM");
		columns.add("ED_INST_TYPE");
		columns.add("FOA_NUMBER");
		columns.add("FULL_PROJECT_NUM");
		columns.add("SUBPROJECT_ID");
		columns.add("FUNDING_ICs");
		columns.add("FY");
		columns.add("IC_NAME");
		columns.add("NIH_SPENDING_CATS");
		columns.add("ORG_CITY");
		columns.add("ORG_COUNTRY");
		columns.add("ORG_DEPT");
		columns.add("ORG_DISTRICT");
		columns.add("ORG_DUNS");
		columns.add("ORG_FIPS");
		columns.add("ORG_NAME");
		columns.add("ORG_STATE");
		columns.add("ORG_ZIPCODE");
		columns.add("PHR");
		columns.add("PI_IDS");
		columns.add("PI_NAMEs");
		columns.add("PROGRAM_OFFICER_NAME");
		columns.add("PROJECT_START");
		columns.add("PROJECT_END");
		columns.add("PROJECT_TERMS");
		columns.add("PROJECT_TITLE");
		columns.add("SERIAL_NUMBER");
		columns.add("STUDY_SECTION");
		columns.add("STUDY_SECTION_NAME");
		columns.add("SUFFIX");
		columns.add("SUPPORT_YEAR");
		columns.add("TOTAL_COST");
		columns.add("TOTAL_COST_SUB_PROJECT");
		columns.add("FUNDING_MECHANISM");
		columns.add("SUBPROJECT_ID");
		columns.add("DIRECT_COST_AMT");
		columns.add("INDIRECT_COST_AMT");	

//		Research Graph schema
		addResearchGraphSchema(columns);
		columns.add("doi");	

		return columns;
	}
	
	private TreeSet<String> getClinicalStudiesColumns(){
		TreeSet<String> columns = new TreeSet<>();
		columns.add("Core Project Number");
		columns.add("ClinicalTrials.gov ID");
		columns.add("Study");
		columns.add("Study Status");

//		Research Graph schema
		addResearchGraphSchema(columns);
		columns.add("doi");
		
		return columns;
	}
	
	private TreeSet<String> getLinkTables(){
		TreeSet<String> columns = new TreeSet<>();
		columns.add("PMID");
		columns.add("PROJECT_NUMBER");
		
		return columns;	
	}
	
	private void addResearchGraphSchema(TreeSet<String> columnList){
//		Research Graph schema
		columnList.add("key");		
		columnList.add("source");	
		columnList.add("last_updated");	
		columnList.add("scopus_eid");	
		columnList.add("type");
	}
	
//	### NIH mapping ###
	public String doHeaderColumnNameMapping(String header, String entityType){
		header = header.replace("AUTHOR_LIST", "author_list");
		header = header.replace("ISSN", "nih_issn");
		header = header.replace("PMID", "local_id");
		header = header.replace("PUB_TITLE", "title");
		header = header.replace("PUB_YEAR", "publication_year");	
			
		header = header.replace("APPLICATION_ID", "nih_application_id");
		header = header.replace("ACTIVITY", "nih_activity");
		header = header.replace("ADMINISTERING_IC", "nih_administering_ic");
		header = header.replace("APPLICATION_TYPE", "nih_application_type");
		header = header.replace("ARRA_FUNDED", "nih_arra_funded");
		header = header.replace("AWARD_NOTICE_DATE", "nih_award_notice_date");
		header = header.replace("BUDGET_START", "nih_budget_start");
		header = header.replace("BUDGET_END", "nih_budget_end");
		header = header.replace("CFDA_CODE", "nih_cfda_code");
		header = header.replace("CORE_PROJECT_NUM", "local_id");
		header = header.replace("ED_INST_TYPE", "nih_ed_inst_type");
		header = header.replace("FOA_NUMBER", "nih_foa_number");
		header = header.replace("FULL_PROJECT_NUM", "nih_full_project_num");
		header = header.replace("SUBPROJECT_ID", "nih_subproject_id");
		header = header.replace("FUNDING_ICs", "nih_funding_ics");
		header = header.replace("FY", "nih_fy");
		header = header.replace("IC_NAME", "nih_ic_name");
		header = header.replace("NIH_SPENDING_CATS", "nih_spending_cats");
		header = header.replace("ORG_CITY", "nih_org_city");
		header = header.replace("ORG_COUNTRY", "nih_org_country");
		header = header.replace("ORG_DEPT", "nih_org_dept");
		header = header.replace("ORG_DISTRICT", "nih_org_district");
		header = header.replace("ORG_DUNS", "nih_org_duns");
		header = header.replace("ORG_FIPS", "nih_org_fips");
		header = header.replace("ORG_NAME", "nih_org_name");
		header = header.replace("ORG_STATE", "nih_org_state");
		header = header.replace("ORG_ZIPCODE", "nih_org_zipcode");
		header = header.replace("PHR", "nih_phr");
		header = header.replace("PI_IDS", "nih_pi_ids");
		header = header.replace("PI_NAMEs", "participant_list");
		header = header.replace("PROGRAM_OFFICER_NAME", "nih_program_officer_name");
		header = header.replace("PROJECT_START", "start_year");
		header = header.replace("PROJECT_END", "end_year");
		header = header.replace("PROJECT_TERMS", "nih_project_terms");
		header = header.replace("PROJECT_TITLE", "title");
		header = header.replace("SERIAL_NUMBER", "nih_serial_number");
		header = header.replace("STUDY_SECTION", "nih_study_section");
		header = header.replace("STUDY_SECTION_NAME", "nih_study_section_name");
		header = header.replace("SUFFIX", "nih_suffix");
		header = header.replace("SUPPORT_YEAR", "nih_support_year");
		header = header.replace("TOTAL_COST", "nih_total_cost");
		header = header.replace("TOTAL_COST_SUB_PROJECT", "nih_total_cost_sub_project");
		header = header.replace("FUNDING_MECHANISM", "nih_funding_mechanism");
		header = header.replace("SUBPROJECT_ID", "nih_subproject_id");
		header = header.replace("DIRECT_COST_AMT", "nih_direct_cost_amt");
		header = header.replace("INDIRECT_COST_AMT", "nih_indirect_cost_amt");	
		

		header = header.replace("Core Project Number", "nih_core_project_number");
		header = header.replace("ClinicalTrials.gov ID", "local_id");
		header = header.replace("Study", "title");
		header = header.replace("Study Status", "nih_study_status");
		
		return header;
	}
	
	public String doRelationshipHeaderColumnNameMapping(String header){
		header = header.replace("PMID", ":START_ID");
		header = header.replace("PROJECT_NUMBER", ":END_ID");
		return header;
	}
	
//	research graph values
	public void addResearchGraphValues(String[] orderedElements, HashMap<String, Integer> columnsWithIndexes, String local_id, String currentEntityType){
		Integer columnIndex = columnsWithIndexes.get("key");
		if(columnIndex != null && columnIndex >= 0)
			orderedElements[columnIndex] = "researchgraph.org/nih/" + local_id;
		
		columnIndex = columnsWithIndexes.get("source");
		if(columnIndex != null && columnIndex >= 0)
			orderedElements[columnIndex] = "nih.gov";
		
		columnIndex = columnsWithIndexes.get("last_updated");
		if(columnIndex != null && columnIndex >= 0)
			orderedElements[columnIndex] = this.currentDateAndTime;
		
		columnIndex = columnsWithIndexes.get("scopus_eid");
		if(columnIndex != null && columnIndex >= 0)
			orderedElements[columnIndex] = "\"\"";
		
		columnIndex = columnsWithIndexes.get("type");
		if(columnIndex != null && columnIndex >= 0)
			orderedElements[columnIndex] = getLabel(currentEntityType);
	}
	
	public void addRelationshipResearchGraphValues(String[] orderedElements, HashMap<String, Integer> columnsWithIndexes){
		Integer columnIndex = columnsWithIndexes.get(":TYPE");
		if(columnIndex != null && columnIndex >= 0)
			orderedElements[columnIndex] = getLabel(EntityTypes.entityLink.toString());
	}
	
//	### labels ###
	
	public String getLabel(String entityType){
		String label = "";
		
		EntityTypes type = EntityTypes.valueOf(entityType);
		
		switch(type){
		case project: label = "grant"; break;
		case publication: label = "publication"; break;
		case clinicalStudy: label = "dataset"; break;
		case entityLink: label = "relatedTo";
		}		
		
		return label;
	}
	
	
	
	
}
