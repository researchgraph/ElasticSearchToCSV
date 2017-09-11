package json2csv;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.TimeZone;
import java.util.TreeSet;

import org.apache.commons.io.FileUtils;

import tools.Tools;

public class Merge {
	private String csvSeparator = ",";
	
	private HashMap<String, HashMap<String, Integer>> columnIndexMap;
	private HashMap<String, String> headerMap;
	private String currentDateAndTime;

	private HashMap<String, Integer> relColumnIndexMap;
	String relationshipHeader;
	
	private Mode mode;
	
	private String currentEntityType = "";
	
	public Merge(Mode mode){
		this.mode = mode;
		if(mode == Mode.entityLink){
			setRelationshipIndicesAndHeaders();
		}
		else{
			setCurrentDateAndTime();
			setColumnIndicesAndHeaders();
		}
	}
	
	private void setCurrentDateAndTime(){
		TimeZone timeZone = TimeZone.getTimeZone("UTC");
		Calendar calendar = Calendar.getInstance(timeZone);

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("EEE MMM dd hh:mm:ss 'UTC' yyyy", Locale.US);
		simpleDateFormat.setTimeZone(timeZone);
		
		String time = simpleDateFormat.format(calendar.getTime());
		
		this.currentDateAndTime = time;
	}
	
	
//	### headers and mappings #######################################
	
	public enum Mode{
		entity, entityLink;
	}
	
	public enum EntityTypes{
		
		citedData("citedData"),
		dataset("dataset"),
		institution("institution"),
		instrument("instrument"),
		project("project"),
		publication("publication");
		
		public final String name;
		
		private EntityTypes(String s){
			this.name = s;
		}
		
		public String toString(){
			return this.name();
		}
	}
	
	
	private void setColumnIndicesAndHeaders(){
		TreeSet<String> citedDataColumns = getCitedDataColumns();
		TreeSet<String> datasetColumns = getDatasetColumns();
		TreeSet<String> institutionColumns = getInstitutionColumns();
		TreeSet<String> instrumentColumns = getInstrumentColumns();
		TreeSet<String> projectColumns = getProjectColumns();
		TreeSet<String> publicationColumns = getPublicationColumns();
		
		columnIndexMap = new HashMap<>();
		columnIndexMap.put(EntityTypes.citedData.toString(), defineEntityIndexes(citedDataColumns));
		columnIndexMap.put(EntityTypes.dataset.toString(), defineEntityIndexes(datasetColumns));
		columnIndexMap.put(EntityTypes.institution.toString(), defineEntityIndexes(institutionColumns));
		columnIndexMap.put(EntityTypes.instrument.toString(), defineEntityIndexes(instrumentColumns));
		columnIndexMap.put(EntityTypes.project.toString(), defineEntityIndexes(projectColumns));
		columnIndexMap.put(EntityTypes.publication.toString(), defineEntityIndexes(publicationColumns));
		
		headerMap = new HashMap<>();
		headerMap.put(EntityTypes.citedData.toString(), createHeader(citedDataColumns, EntityTypes.citedData));
		headerMap.put(EntityTypes.dataset.toString(), createHeader(datasetColumns, EntityTypes.dataset));
		headerMap.put(EntityTypes.institution.toString(), createHeader(institutionColumns, EntityTypes.institution));
		headerMap.put(EntityTypes.instrument.toString(), createHeader(instrumentColumns, EntityTypes.instrument));
		headerMap.put(EntityTypes.project.toString(), createHeader(projectColumns, EntityTypes.project));
		headerMap.put(EntityTypes.publication.toString(), createHeader(publicationColumns, EntityTypes.publication));
	}
	
	private void setRelationshipIndicesAndHeaders(){
		TreeSet<String> relationshipColumns = getRelationshipColumns();		
		relColumnIndexMap = defineEntityIndexes(relationshipColumns);
		relationshipHeader = createHeader(relationshipColumns, null);
	}
	
	
//	### columns ###
	
	private TreeSet<String> getCitedDataColumns(){
		TreeSet<String> columns = new TreeSet<>();
//		columns.add("_id");
		columns.add("_index");
//		columns.add("_score");
		columns.add("_source.entityProvenance");
		columns.add("_source.entityReliability");
		columns.add("_source.entityType");
//		columns.add("_source.entityView");
		columns.add("_source.freeKeywords");
		columns.add("_source.gwsId");
		columns.add("_source.name");
		columns.add("_source.numericInfo");
		columns.add("_source.year");
		columns.add("_type");
		
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
		columns.add("key");		
		columns.add("source");	
		columns.add("last_updated");	
		columns.add("scopus_eid");	
		columns.add("type");
		
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
	
//	### indices ###
	
	private HashMap<String, Integer> defineEntityIndexes(TreeSet<String> columns){
		return defineColumnIndexes(columns);
	}
	
	private HashMap<String, Integer> defineColumnIndexes(TreeSet<String> columns){
		HashMap<String, Integer> columnsWithIndexes = new HashMap<>();
		
		int index = 0;
		for(String column: columns){
			columnsWithIndexes.put(column, index++);
		}		
		
		return columnsWithIndexes;
	}
	
//	### headers ####
	
	private String createHeader(TreeSet<String> columns, EntityTypes entityType){
		String header = Tools.getArrayAsString(columns, csvSeparator);
		return prepareHeader(header, entityType);
	}
	
	private String prepareHeader(String header, EntityTypes entityType){
		header = header.replace("_source.", "");		
		if(entityType == null)
			header = doRelationshipHeaderColumnNameMapping(header);
		else
			header = doHeaderColumnNameMapping(header, entityType);
		
		return header;
	}
	
//	### mapping ###
	public String doHeaderColumnNameMapping(String header, EntityTypes entityType){
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
		if(entityType == EntityTypes.project)
			header = header.replace("year", "start_year");/*in rg-schema*/
		else
			header = header.replace("year", "publication_year");/*in rg-schema*/
		
		
		return header;
	}
	
	public String doRelationshipHeaderColumnNameMapping(String header){
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
	
	
	private void addResearchGraphValues(String[] orderedElements, HashMap<String, Integer> columnsWithIndexes, String local_id){
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
			orderedElements[columnIndex] = getLabel(this.currentEntityType);
	}
	
	private void addRelationshipResearchGraphValues(String[] orderedElements, HashMap<String, Integer> columnsWithIndexes){
		Integer columnIndex = columnsWithIndexes.get(":TYPE");
		if(columnIndex != null && columnIndex >= 0)
			orderedElements[columnIndex] = "relatedTo";
	}
	
	
//	### labels ###
	public String getLabel(String entityName){
		EntityTypes entityType = EntityTypes.valueOf(entityName);
		return getLabel(entityType);
	}
	
	public String getLabel(EntityTypes entityType){
		String label = "";
		
		switch(entityType){
		case citedData: label = "gesis|citedData"; break;
		case dataset: label = "dataset"; break;
		case institution: label = "institution"; break;
		case instrument: label = "gesis|instrument"; break;
		case project: label = "grant"; break;
		case publication: label = "publication";
		}		
		
		return label;
	}
	
//	### merge csv files #########################################
	public void mergeCsvFiles(String csvDirPath, String csvOutputPath){
		HashSet<String> entityTypes = new HashSet<>();
		
		String csvOutputPathPrefix = csvOutputPath.substring(0, csvOutputPath.lastIndexOf("."));
		String csvOutputPathSuffix = csvOutputPath.substring(csvOutputPath.lastIndexOf("."));
		String currentTypeOutputPath;
		
		File[] files = Tools.getFilesFromDir(csvDirPath);
		String content, header;
		String[] fileColumns;
		String[] rows;
		
		String orderedRowContent = "";
		
		File outFile = null;
		
		if(this.mode == Mode.entityLink){
			outFile = new File(csvOutputPath);
		}
		
		for(File inFile: files){
			try {
				content = FileUtils.readFileToString(inFile, "UTF-8");
				
				header = content.substring(0, content.indexOf("\n"));
				fileColumns = header.split(csvSeparator);
				
				content = content.substring(content.indexOf("\n") +1);
				rows = content.split("\n");
				
				try {					
					if(this.mode == Mode.entityLink){						
						this.currentEntityType = Mode.entityLink.toString();
//						add header if it's the first iteration
						if(entityTypes.add(this.currentEntityType))
							FileUtils.writeStringToFile(outFile, this.relationshipHeader + "\n", "UTF-8");
					}
					
					for(String row: rows){
						if(this.mode == Mode.entity){
							orderedRowContent = getOrderedRow(row, fileColumns) + "," + getLabel(this.currentEntityType) + "\n";
							currentTypeOutputPath = csvOutputPathPrefix + "_" + currentEntityType + csvOutputPathSuffix;
							outFile = new File(currentTypeOutputPath);
							
	//						add header if it's the first occurrence of the current entityType
							if(entityTypes.add(this.currentEntityType))
								FileUtils.writeStringToFile(outFile, this.headerMap.get(this.currentEntityType) + ",:LABEL" + "\n", "UTF-8");							
								
						}
						else if(this.mode == Mode.entityLink){
							orderedRowContent = getOrderedRow(row, fileColumns) + "\n";							
						}
						
						FileUtils.writeStringToFile(outFile, orderedRowContent, "UTF-8", true);		
					}				
				} catch (IOException e1) {
					e1.printStackTrace();
				}				
				
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		
	}	
	
	
	public String getOrderedRow(String unorderedRow, String[] fileHeader){
		CSVCleaner cleaner = new CSVCleaner();		
		ArrayList<String> rowElements = cleaner.getElements(unorderedRow);	
		HashMap<String, Integer> columnsWithIndexes;
		
		if(this.mode == Mode.entity){
			this.currentEntityType = getCurrentEntityType(fileHeader, rowElements);
			columnsWithIndexes = this.columnIndexMap.get(this.currentEntityType);
		}
		else{
			columnsWithIndexes = this.relColumnIndexMap;
		}
		
		String[] orderedElements = Tools.getInitializedStringArray(columnsWithIndexes.size());
		Integer columnIndex;
		String local_id = "";
		
		for(int i = 0; i < fileHeader.length; i++){
			columnIndex = columnsWithIndexes.get(fileHeader[i].trim());
			if(columnIndex != null && columnIndex >= 0){
				orderedElements[columnIndex] = rowElements.get(i).trim();
				
				if(orderedElements[columnIndex].equals("null"))
					orderedElements[columnIndex] = "\"\"";
				
				if(fileHeader[i].contains("gwsId"))
					local_id = orderedElements[columnIndex];
			}
		}
		
		if(this.mode == Mode.entity)
			addResearchGraphValues(orderedElements, columnsWithIndexes, local_id);
		else
			addRelationshipResearchGraphValues(orderedElements, columnsWithIndexes);
		
		
		return Tools.getArrayAsString(orderedElements, csvSeparator);
	}
	
	private String getCurrentEntityType(String[] fileHeader, ArrayList<String> rowElements){
		for(int i = 0; i < fileHeader.length; i++){
			if(fileHeader[i].equals("_source.entityType"))
				return rowElements.get(i).trim();
		}
		
		return null;
	}	
	
	
	public void mergeFiles(File[] sourceFiles, String resultFilePath) throws IOException{
		File resultFile = new File(resultFilePath);
		if(!resultFile.exists()){
			resultFile.createNewFile();
		}

		System.out.println("files: " + sourceFiles.length);
		String fileContent;
		
		for(File file: sourceFiles){
			fileContent = FileUtils.readFileToString(file, "UTF-8");
			FileUtils.writeStringToFile(resultFile, fileContent, "UTF-8", true);
		}
	}
}
