package mappings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeSet;

import json2csv.Merge.Mode;


public class GESIS_Mapping extends Mapping{
		
	
	public GESIS_Mapping(String csvSeparator, String currentDateAndTime, Mode mode){
		super(csvSeparator, currentDateAndTime, mode);
		setLocalIDFieldName("gwsID");
	}
	
	public enum EntityTypes{		
		citedData,
		dataset,
		institution,
		instrument,
		project,
		publication,
		entityLink;
	}	
	
	protected String[] getNodeEntitiesNames(){
		return new String[]{
				EntityTypes.citedData.toString(),
				EntityTypes.dataset.toString(),
				EntityTypes.institution.toString(),
				EntityTypes.instrument.toString(),
				EntityTypes.project.toString(),
				EntityTypes.publication.toString()
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
		case citedData: return getCitedDataColumns();
		case dataset: return getDatasetColumns();
		case institution: return  getInstitutionColumns();
		case instrument: return getInstrumentColumns();
		case project: return getProjectColumns();
		case publication: return getPublicationColumns();
		case entityLink: return getRelationshipColumns();
		}
		
		return null;
	}
	
	public String getCurrentEntityType(String[] columnNames, ArrayList<String> rowElements){
		for(int i = 0; i < columnNames.length; i++){
			if(columnNames[i].equals("_source.entityType"))
				return rowElements.get(i).trim();
		}
		
		return null;
	}
	
	
//	### columns (names from source) ###
	
//	private TreeSet<String> getCitedDataColumns(){
//		TreeSet<String> columns = new TreeSet<>();
////		columns.add("_id");
//		columns.add("_index");
////		columns.add("_score");
//		columns.add("_source.entityProvenance");
//		columns.add("_source.entityReliability");
//		columns.add("_source.entityType");
////		columns.add("_source.entityView");
//		columns.add("_source.freeKeywords");
//		columns.add("_source.gwsId");
//		columns.add("_source.name");
//		columns.add("_source.numericInfo");
//		columns.add("_source.year");
//		columns.add("_type");
//		
//		columns.add("type");
//		
//		return columns;
//	}
	
	private TreeSet<String> getCitedDataColumns(){
		TreeSet<String> columns = new TreeSet<>();
//		columns.add("_id");
		columns.add("_index");
//		columns.add("_score");
		columns.add("_source.entityProvenance");
		columns.add("_source.entityReliability");
		columns.add("_source.entityType");
//		columns.add("_source.entityView");
//		columns.add("_source.freeKeywords");
		columns.add("_source.gwsId");
		columns.add("_source.name");
		columns.add("_source.numericInfo");
		columns.add("_source.year");
		columns.add("_type");
		
//		Research Graph schema
		columns.add("key");		
		columns.add("source");	
		columns.add("last_updated");	
		columns.add("licence");	
		columns.add("megabyte");
		columns.add("type");
		
		return columns;
	}
	private TreeSet<String> getDatasetColumns(){
		TreeSet<String> columns = new TreeSet<>();
//		columns.add("_id");
		columns.add("_index");
//		columns.add("_score");
//		columns.add("_source.abstractText");
		columns.add("_source.authors");
		columns.add("_source.doi");
		columns.add("_source.entityProvenance");
		columns.add("_source.entityReliability");
		columns.add("_source.entityType");
//		columns.add("_source.entityView");
		columns.add("_source.gwsId");
//		columns.add("_source.identifiers");
		columns.add("_source.name");
		columns.add("_source.numericInfo");
		columns.add("_source.publisher");
		columns.add("_source.year");
		columns.add("_type");
		
//		Research Graph schema
		columns.add("key");		
		columns.add("source");	
		columns.add("last_updated");	
		columns.add("licence");	
		columns.add("megabyte");
		columns.add("type");
		
		return columns;
	}
	
	private TreeSet<String> getInstitutionColumns(){
		TreeSet<String> columns = new TreeSet<>();
//		columns.add("_id");
		columns.add("_index");
//		columns.add("_score");
		columns.add("_source.entityProvenance");
		columns.add("_source.entityReliability");
		columns.add("_source.entityType");
//		columns.add("_source.entityView");
		columns.add("_source.gwsId");
//		columns.add("_source.identifiers");
		columns.add("_source.name");
		columns.add("_type");		

		columns.add("type");
		
		return columns;
	}
	
	private TreeSet<String> getInstrumentColumns(){
		TreeSet<String> columns = new TreeSet<>();
//		columns.add("_id");
		columns.add("_index");
//		columns.add("_score");
		columns.add("_source.doi");
		columns.add("_source.entityProvenance");
		columns.add("_source.entityReliability");
		columns.add("_source.entityType");
//		columns.add("_source.entityView");
		columns.add("_source.gwsId");
//		columns.add("_source.identifiers");
		columns.add("_source.name");
		columns.add("_source.url");
		columns.add("_type");		

		columns.add("type");
		
		return columns;
	}
	
	private TreeSet<String> getProjectColumns(){
		TreeSet<String> columns = new TreeSet<>();
//		columns.add("_id");
		columns.add("_index");
//		columns.add("_score");
//		columns.add("_source.abstractText");
		columns.add("_source.authors");
		columns.add("_source.entityProvenance");
		columns.add("_source.entityReliability");
		columns.add("_source.entityType");
//		columns.add("_source.entityView");
		columns.add("_source.gwsId");
//		columns.add("_source.identifiers");
		columns.add("_source.name");
		columns.add("_source.numericInfo");
		columns.add("_source.spatial");
		columns.add("_source.url");
		columns.add("_source.year");
		columns.add("_type");
		
//		Research Graph schema
		columns.add("key");		
		columns.add("source");	
		columns.add("last_updated");	
		columns.add("purl");	
		columns.add("participant_list");
		columns.add("funder");		
		columns.add("end_year");
		columns.add("type");
		
		return columns;
	}
	
	private TreeSet<String> getPublicationColumns(){
		TreeSet<String> columns = new TreeSet<>();
//		columns.add("_id");
		columns.add("_index");
//		columns.add("_score");
		columns.add("_source.authors");
		columns.add("_source.collectionTitle");
		columns.add("_source.doi");
		columns.add("_source.editors");
		columns.add("_source.entityProvenance");
		columns.add("_source.entityReliability");
		columns.add("_source.entityType");
//		columns.add("_source.entityView");
		columns.add("_source.gwsId");
//		columns.add("_source.identifiers");
		columns.add("_source.isbn");
		columns.add("_source.issn");
		columns.add("_source.journalTitle");
		columns.add("_source.language");
		columns.add("_source.location");
		columns.add("_source.month");
		columns.add("_source.name");
		columns.add("_source.number");
		columns.add("_source.pages");
		columns.add("_source.publicationStatus");
		columns.add("_source.publicationType");
		columns.add("_source.publisher");
		columns.add("_source.seriesTitle");
		columns.add("_source.url");
		columns.add("_source.volume");
		columns.add("_source.year");
		columns.add("_type");
		
//		Research Graph schema
		addResearchGraphSchema(columns);
		
		return columns;
	}
	
	private TreeSet<String> getRelationshipColumns(){
		TreeSet<String> columns = new TreeSet<>();
		columns.add("_source.tags");
		columns.add("_index");
		columns.add("_source.gws_fromType");
		columns.add("_source.gws_fromID");
		columns.add("_type");
//		columns.add("_source.linkView");
//		columns.add("_source.gws_fromView");
//		columns.add("_score");
//		columns.add("_source.gws_toView");
		columns.add("_source.gws_link");
		columns.add("_source.entityRelations");
		columns.add("_source.gws_toType");
		columns.add("_source.linkReason");
		columns.add("_source.gws_toID");
		columns.add("_id");
//		columns.add("_source.toEntity");
//		columns.add("_source.confidence");
		columns.add("_source.provenance");
//		columns.add("_source.fromEntity");
		
		columns.add(":TYPE");
		
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
	
	
	
//	### mapping ###
	public String doHeaderColumnNameMapping(String header, String entityType){
		header = header.replace("_source.", "");
//		header = header.replace("_id", "");
		header = header.replace("_index", "gesis_index");
//		header = header.replace("_score", "");
//		header = header.replace("abstractText", "");
		header = header.replace("_type", "gesis_type");
		header = header.replace("alternativeNames", "gesis_alternative_names");
		header = header.replace("authors", "authors_list");/*in rg-schema*/
		header = header.replace("classification", "gesis_classification");
		header = header.replace("collectionTitle", "gesis_collection_title");
		header = header.replace("doi", "doi");/*in rg-schema*/
		header = header.replace("editors", "gesis_editors");
		header = header.replace("entityProvenance", "gesis_entity_provenance");
		header = header.replace("entityReliability", "gesis_entity_reliability");
		header = header.replace("entityType", "gesis_entity_type");
		header = header.replace("entityView", "gesis_entity_view");
		header = header.replace("freeKeywords", "gesis_free_keywords");
		header = header.replace("gwsId", "local_id:ID");/*in rg-schema*/
		header = header.replace("identifiers", "gesis_identifiers");
		header = header.replace("isbn", "gesis_isbn");
		header = header.replace("issn", "gesis_issn");
		header = header.replace("journalTitle", "gesis_journal_title");
		header = header.replace("language", "gesis_language");
		header = header.replace("location", "gesis_location");
		header = header.replace("methodKeywords", "gesis_method_keywords");
		header = header.replace("month", "gesis_month");
		header = header.replace("name", "title");/*in rg-schema*/
		header = header.replace("number", "gesis_number");
		header = header.replace("numericInfo", "gesis_numeric_info");
		header = header.replace("pages", "gesis_pages");
		header = header.replace("publicationStatus", "gesis_publication_status");
		header = header.replace("publicationType", "gesis_publication_type");
		header = header.replace("publisher", "gesis_publisher");
		header = header.replace("seriesTitle", "gesis_series_title");
		header = header.replace("spatial", "gesis_spatial");
		header = header.replace("subjects", "gesis_subjects");
		header = header.replace("tags", "gesis_tags");
		header = header.replace("textualReferences", "gesis_textual_references");
		header = header.replace("url", "url");/*in rg-schema*/
		header = header.replace("volume", "gesis_volume");
		if(entityType.equals(EntityTypes.project.toString()))
			header = header.replace("year", "start_year");/*in rg-schema*/
		else
			header = header.replace("year", "publication_year");/*in rg-schema*/
		
		
		return header;
	}
		
	
	public String doRelationshipHeaderColumnNameMapping(String header){
		header = header.replace("_source.", "");
		header = header.replace("_type", "gesis_type");
		header = header.replace("tags", "gesis_tags");
		header = header.replace("_index", "gesis_index");
		header = header.replace("gws_fromType", "gesis_from_type");
		header = header.replace("gws_fromID", ":START_ID");
//		header = header.replace("linkView", "");
//		header = header.replace("gws_fromView", "");
//		header = header.replace("_score", "");
//		header = header.replace("gws_toView", "");
		header = header.replace("gws_link", "gesis_link");
		header = header.replace("entityRelations", "gesis_entity_relations");
		header = header.replace("gws_toType", "gesis_to_type");
		header = header.replace("linkReason", "gesis_link_reason");
		header = header.replace("gws_toID", ":END_ID");
		header = header.replace("_id", "gesis_id");
//		header = header.replace("toEntity", "");
//		header = header.replace("confidence", "");
		header = header.replace("provenance", "gesis_provenance");
//		header = header.replace("fromEntity", "");
		
		return header;
	}
	
	
//	research graph values
	public void addResearchGraphValues(String[] orderedElements, HashMap<String, Integer> columnsWithIndexes, String local_id, String currentEntityType){
		Integer columnIndex = columnsWithIndexes.get("key");
		if(columnIndex != null && columnIndex >= 0)
			orderedElements[columnIndex] = "researchgraph.org/gesis/" + local_id;
		
		columnIndex = columnsWithIndexes.get("source");
		if(columnIndex != null && columnIndex >= 0)
			orderedElements[columnIndex] = "gesis.org";
		
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
		case citedData: label = "dataset"; break;
		case dataset: label = "dataset"; break;
		case institution: label = "institution"; break;
		case instrument: label = "gesis_institution"; break;
		case project: label = "grant"; break;
		case publication: label = "publication"; break;
		case entityLink: label = "relatedTo";
		}		
		
		return label;
	}
	

	
}
