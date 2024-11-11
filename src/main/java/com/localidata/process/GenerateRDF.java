package com.localidata.process;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import com.localidata.bean.ConfigBean;
import com.localidata.bean.DataBean;
import com.localidata.bean.SkosBean;
import com.localidata.generic.Constants;
import com.localidata.generic.Prop;
import com.localidata.util.Utils;
import com.localidata.util.OpenTelemetryConfig;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.context.Context;


/**
 * 
 * @author Localidata
 */
public class GenerateRDF {
	private final static Logger log = Logger.getLogger(GenerateRDF.class);
	private static final Tracer tracer = OpenTelemetryConfig.getTracer();

	private String inputDirectoryString = "";
	private String outputDirectoryString = "";
	private String configDirectoryString = "";
	private String urlsFileString;
	private String[] extensions = new String[] { "csv" };
	private String[] extensionsConfig = new String[] { "xlsx", "csv" };
	private String[] extensionsZip = new String[] { "ttl" };
	private HashMap<String, ConfigBean> mapconfig = new HashMap<String, ConfigBean>();
	private ArrayList<DataBean> dataWithSkos = new ArrayList<DataBean>();
	private ArrayList<DataBean> dataWithSkosHierarchical = new ArrayList<DataBean>();
	private ArrayList<String> dsdList = new ArrayList<String>();
	private ArrayList<String> propertiesList = new ArrayList<String>();
	private HashSet<String> filesNotRDF = new HashSet<>();
	private HashMap<String, String> idDescription;
	private String specsTtlFileString;

	public GenerateRDF(String input, String output, String config, String urls, String specsTtl) {
		this.inputDirectoryString = input;
		this.outputDirectoryString = output;
		this.configDirectoryString = config;
		this.urlsFileString = urls;
		this.specsTtlFileString = specsTtl;
	}

	public void readConfig(HashMap<String, String> idDescription) {
		log.debug("Init readConfig");
		
		log.info("Comienza a extraerse la configuración");

		Span readConfigSpan = tracer.spanBuilder("Read Configuration")
                                .setSpanKind(SpanKind.INTERNAL)
                                .startSpan();

		try (Scope scopeReadConfigSpan = readConfigSpan.makeCurrent()) {
			File configDirectoryFile = new File(this.configDirectoryString);
			File areasReportFile = new File(String.valueOf(this.outputDirectoryString) + File.separator + "areas.txt");
			Collection<File> listCSV = FileUtils.listFiles(configDirectoryFile, this.extensionsConfig, true);
			int cont = 0;
			
			int size = listCSV.size();
			for (File file : listCSV) {
				log.info("Se extrae el fichero " + file.getName() + " " + (++cont) + " " + size);
				if (!file.getName().startsWith("mapping") && !file.getName().startsWith(Prop.fileHashCSV)) {

					Span fileProcessingSpan = tracer.spanBuilder("Process Config File: " + file.getName())
						.setParent(Context.current().with(readConfigSpan))
						.setSpanKind(SpanKind.INTERNAL)
						.startSpan();

					try (Scope fileScope = fileProcessingSpan.makeCurrent()) {

						fileProcessingSpan.setAttribute("file.name", file.getName());

						ConfigBean configBean = new ConfigBean();
						configBean.setNameFile(file.getName());
						String id = file.getName().substring(8);
						String areas = "";
						id = id.replace(".csv", "");
						id = id.replace(".xlsx", "");
						if (id.contains("TC")) {
							id = id.replace("TC", "");
							configBean.getLetters().add("TC");
							areas += "TC ";
						}
						if (id.contains("TM")) {
							id = id.replace("TM", "");
							configBean.getLetters().add("TM");
							areas += "TM ";
						}
						if (id.contains("TP")) {
							id = id.replace("TP", "");
							configBean.getLetters().add("TP");
							areas += "TP ";
						}
						if (id.contains("A")) {
							
							id = id.substring(0, id.lastIndexOf("A"))+id.substring(id.lastIndexOf("A")+1, id.length());
							configBean.getLetters().add("A");
							areas += "A ";
						}
						while (id.charAt(id.length() - 1) == '-') {
							id = id.substring(0, id.length() - 1);
						}

						configBean.setId(id);
						if (Prop.formatConfig.equals("csv")) {
							readCsv(file, configBean);
						} else {
							readXlsxFile(file, configBean);
						}
						mapconfig.put(id, configBean);
						Utils.stringToFileAppend(id + " " + areas + "\n", areasReportFile);

					} finally {
                    	fileProcessingSpan.end();
                	}
				}
			}

			for (Iterator<DataBean> it1 = dataWithSkosHierarchical.iterator(); it1.hasNext();) {
				DataBean data1 = (DataBean) it1.next();
				if (data1.getRelationKos() != null) {
					DataBean data2 = mapconfig.get(data1.getIdConfig()).getMapData().get(data1.getRelationKos());
					HashMap<String, SkosBean> mapSkos = data1.mergeSkos(data2);
					if (mapSkos != null) {
						try{
							log.info("Kos " + data1.getName() + " is parent of " + data2.getName());
							data2.setWriteSkos(false);
							data1.setMapSkos(mapSkos);
							data2.setMapSkos(mapSkos);
							mapconfig.get(data1.getIdConfig()).getMapData().get(data1.getNameNormalized()).setMapSkos(mapSkos);
							mapconfig.get(data2.getIdConfig()).getMapData().get(data2.getNameNormalized()).setMapSkos(mapSkos);
							data1.setNormalizacion(data1.getNormalizacion().replace(data1.getNameNormalized(), data1.getKosNameNormalized()));
							data2.setNormalizacion(data2.getNormalizacion().replace(data2.getNameNormalized(), data2.getKosNameNormalized()));
						}
						catch(Exception e){
							log.error("[M]Error : ", e);
						}
					}
				}
			}

			if (idDescription == null) {
				this.idDescription = new HashMap<String, String>();
				File urlsFile = new File(urlsFileString);
				List<String> csvLines;
				try {
					csvLines = FileUtils.readLines(urlsFile, "UTF-8");
					for (int h = 1; h < csvLines.size(); h++) {
						String line = csvLines.get(h);
						String[] valores = line.split(",");
						valores[0] = valores[0].replaceAll("\"", "");
						valores[1] = valores[1].replaceAll("\"", "");
						valores[2] = valores[2].replaceAll("\"", "");
						this.idDescription.put(valores[1], valores[2]);
					}
				} catch (IOException e) {
					log.error("Error leyendo la configuración", e);
				}

			} else {
				this.idDescription = idDescription;
			}

			log.info("Finaliza de extraerse la configuración");
			log.debug("End readConfig");
			readConfigSpan.addEvent("Finaliza de extraerse la configuración");

		}finally {
			readConfigSpan.end();
		}
	}

	private void readCsv(File file, ConfigBean configBean) {

		Span readCsvSpan = tracer.spanBuilder("Read CSV File: " + file.getName())
                             .setSpanKind(SpanKind.INTERNAL)
                             .startSpan();


		log.debug("Init readCsv");
		List<String> csvLines;
		try (Scope scopeReadCsvSpan = readCsvSpan.makeCurrent()) {

			csvLines = FileUtils.readLines(file, "UTF-8");

			String[] cellsName = csvLines.get(0).split(",");
			String[] cellsNameNormalized = csvLines.get(1).split(",");
			String[] cellsNormalization = csvLines.get(2).split(",");
			String[] cellsDimMesure = csvLines.get(3).split(",");
			String[] cellsType = csvLines.get(4).split(",");
			String[] cellsSkosfile = csvLines.get(5).split(",");
			String[] cellsConstant = null;
			String[] cellsConstantValue = null;
			String[] cellsRelationKos = null;
			String[] cellsKosName = null;
			String[] cellsKosNameNormalized = null;

			if (csvLines.size() == 7)
				cellsConstant = csvLines.get(6).split(",");
			if (csvLines.size() == 8)
				cellsConstantValue = csvLines.get(7).split(",");
			if (csvLines.size() == 9)
				cellsRelationKos = csvLines.get(9).split(",");
			if (csvLines.size() == 10)
				cellsKosName = csvLines.get(10).split(",");
			if (csvLines.size() == 11)
				cellsKosNameNormalized = csvLines.get(11).split(",");

			int columnReaded = 0;

			readCsvSpan.setAttribute("file.row_count", csvLines.size());
			readCsvSpan.setAttribute("config.id", configBean.getId());

			while (columnReaded < cellsName.length) {

				Span columnSpan = tracer.spanBuilder("Process CSV Column: " + columnReaded)
                                    .setSpanKind(SpanKind.INTERNAL)
                                    .startSpan();
				try (Scope columnScope = columnSpan.makeCurrent()) {
					DataBean dataBean = new DataBean();
					if (cellsName[columnReaded] == null) {
						columnReaded++;
					} else {
						dataBean.setName(removeStartEndCaracter(cellsName[columnReaded]));
						dataBean.setNameNormalized(removeStartEndCaracter(cellsNameNormalized[columnReaded]));
						dataBean.setNormalizacion(removeStartEndCaracter(cellsNormalization[columnReaded]));
						dataBean.setDimensionMesure(removeStartEndCaracter(cellsDimMesure[columnReaded]));
						dataBean.setIdConfig(configBean.getId());
						String type = "";
						if (Utils.v(removeStartEndCaracter(cellsType[columnReaded]))) {
							type = removeStartEndCaracter(cellsType[columnReaded]);
						} else {
							type = "xsd:string";
						}
						dataBean.setType(type);
						if (Utils.v(removeStartEndCaracter(cellsSkosfile[columnReaded]))) {
							columnSpan.addEvent("Processing SKOS file for column " + columnReaded);
							HashMap<String, SkosBean> mapSkos = readMappingFileCSV(removeStartEndCaracter(cellsSkosfile[columnReaded]));
							dataBean.setMapSkos(mapSkos);
							configBean.getMapData().put(dataBean.getNameNormalized(), dataBean);
							dataWithSkos.add(dataBean);
						} else {
							configBean.getMapData().put(dataBean.getNameNormalized(), dataBean);
						}
						if (Prop.addDataConstant && cellsConstant != null && Utils.v(removeStartEndCaracter(cellsConstant[columnReaded])) && removeStartEndCaracter(cellsConstant[columnReaded]).equals(Constants.constante)) {
							if (Utils.v(removeStartEndCaracter(cellsConstantValue[columnReaded]))) {
								dataBean.setConstant(removeStartEndCaracter(cellsConstantValue[columnReaded]) + "");
								configBean.getListDataConstant().add(dataBean);
							}
						}
						if (cellsRelationKos != null && Utils.v(removeStartEndCaracter(cellsRelationKos[columnReaded]))) {
							dataBean.setRelationKos(removeStartEndCaracter(cellsRelationKos[columnReaded]));
							dataWithSkosHierarchical.add(dataBean);
						}
						if (cellsKosNameNormalized != null && Utils.v(removeStartEndCaracter(cellsKosNameNormalized[columnReaded]))) {
							dataBean.setKosNameNormalized(removeStartEndCaracter(cellsKosNameNormalized[columnReaded]));
						} else {
							dataBean.setKosNameNormalized(dataBean.getNameNormalized());
						}
						if (cellsKosName != null && Utils.v(removeStartEndCaracter(cellsKosName[columnReaded]))) {
							dataBean.setKosName(removeStartEndCaracter(cellsKosName[columnReaded]));
						} else {
							dataBean.setKosName(dataBean.getName());
						}

						columnSpan.setAttribute("column.name", dataBean.getName());
						columnSpan.setAttribute("column.type", dataBean.getType());
						columnSpan.setAttribute("column.dimension_mesure", dataBean.getDimensionMesure());
						if (dataBean.getConstant() != null) {
							columnSpan.setAttribute("column.constant_value", dataBean.getConstant());
						}
						if (dataBean.getRelationKos() != null) {
							columnSpan.setAttribute("column.relation_kos", dataBean.getRelationKos());
						}

						columnReaded++;
					}
				} finally {
					columnSpan.end();
				}
			}
		} catch (IOException e) {
			log.error("Error reading CSV file", e);
			readCsvSpan.recordException(e);
		} finally {
			readCsvSpan.end();
		}
		log.debug("End readCsv");
	}

	private String removeStartEndCaracter(String csvLine) {
		log.debug("Init removeStartEndCaracter");
		String line = csvLine;
		if (csvLine != null) {
			if (csvLine.startsWith("\""))
				line = csvLine.substring(1, csvLine.length());
			if (csvLine.endsWith("\""))
				line = line.substring(0, line.length() - 1);
		}
		log.debug("End removeStartEndCaracter");
		return line;
	}

	private void readXlsxFile(File file, ConfigBean configBean) {
		log.debug("Init readXlsxFile");

		Span readXlsxFileSpan = tracer.spanBuilder("Read XLSX File: " + file.getName())
                                  .setSpanKind(SpanKind.INTERNAL)
                                  .startSpan();

		readXlsxFileSpan.setAttribute("config.id", configBean.getId());

		try (Scope scopeReadXlsxFileSpan = readXlsxFileSpan.makeCurrent()) {

			InputStream inp = null;
			Workbook wb = null;
			try {
				inp = new FileInputStream(file);
				wb = WorkbookFactory.create(inp);
			} catch (FileNotFoundException e) {
				log.error(e.getMessage());
				readXlsxFileSpan.recordException(e);
			} catch (InvalidFormatException e) {
				log.error(e.getMessage());
				readXlsxFileSpan.recordException(e);
			} catch (IOException e) {
				log.error(e.getMessage());
				readXlsxFileSpan.recordException(e);
			}

			Sheet sheet = wb.getSheetAt(0);
			Row rowName = sheet.getRow(0);
			Row rowNameNormalized = sheet.getRow(1);
			Row rowNormalization = sheet.getRow(2);
			Row rowDimMesure = sheet.getRow(3);
			Row rowType = sheet.getRow(4);
			Row rowSkosfile = sheet.getRow(5);
			Row rowConstant = sheet.getRow(6);
			Row rowConstantValue = sheet.getRow(7);
			Row rowRelationKos = sheet.getRow(8);
			Row rowKosNameNormalized = sheet.getRow(9);
			Row rowKosName = sheet.getRow(10);

			boolean cont = true;
			int columnReaded = 0;
			while (cont) {
				Span columnSpan = tracer.spanBuilder("Process XLSX Column: " + columnReaded)
					.setParent(Context.current().with(readXlsxFileSpan))
					.setSpanKind(SpanKind.INTERNAL)
					.startSpan();

                try (Scope columnScope = columnSpan.makeCurrent()) {

					Cell cellName = rowName.getCell(columnReaded);
					Cell cellNameNormalized = rowNameNormalized.getCell(columnReaded);
					Cell cellNormalization = rowNormalization.getCell(columnReaded);
					Cell cellDimMesure = rowDimMesure.getCell(columnReaded);
					Cell cellType = rowType.getCell(columnReaded);
					Cell cellSkosfile = null;
					if (rowSkosfile != null)
						cellSkosfile = rowSkosfile.getCell(columnReaded);
					Cell cellConstant = null;
					if (rowConstant != null)
						cellConstant = rowConstant.getCell(columnReaded);
					Cell cellConstantValue = null;
					if (rowConstantValue != null)
						cellConstantValue = rowConstantValue.getCell(columnReaded);
					Cell cellRelationKos = null;
					if (rowRelationKos != null)
						cellRelationKos = rowRelationKos.getCell(columnReaded);
					Cell cellKosName = null;
					if (rowKosName != null)
						cellKosName = rowKosName.getCell(columnReaded);
					Cell cellKosNameNormalized = null;
					if (rowKosNameNormalized != null)
						cellKosNameNormalized = rowKosNameNormalized.getCell(columnReaded);

					DataBean dataBean = new DataBean();
					if (cellName == null) {
						if (rowName.getCell((columnReaded + 1)) == null)
							cont = false;
						else
							columnReaded++;
					} else {
						dataBean.setName(cellName.getStringCellValue());
						columnSpan.setAttribute("column.name", dataBean.getName());
						if(cellNameNormalized!=null){
							dataBean.setNameNormalized(cellNameNormalized.getStringCellValue());
						}else{
							log.error("Error in config "+file.getName()+" in cell name normalized");
						}
						if(cellNormalization!=null){
							dataBean.setNormalizacion(cellNormalization.getStringCellValue());
						}else{
							log.error("Error in config "+file.getName()+" in cell normalization");
						}
						if(cellDimMesure!=null){
							dataBean.setDimensionMesure(cellDimMesure.getStringCellValue());
							columnSpan.setAttribute("column.dimension_mesure", dataBean.getDimensionMesure());
						}else{
							log.error("Error in config "+file.getName()+" in cell dim mesure");
						}
						dataBean.setIdConfig(configBean.getId());
						String type = "";
						if (cellType != null) {
							type = cellType.getStringCellValue();
						} else {
							type = "xsd:string";
						}
						dataBean.setType(type);
						columnSpan.setAttribute("column.type", dataBean.getType());
						if (cellSkosfile != null && !cellSkosfile.getStringCellValue().equals("")) {
							HashMap<String, SkosBean> mapSkos = readMappingFile(cellSkosfile.getStringCellValue());
							dataBean.setMapSkos(mapSkos);
							configBean.getMapData().put(dataBean.getNameNormalized(), dataBean);
							dataWithSkos.add(dataBean);
						} else {
							configBean.getMapData().put(dataBean.getNameNormalized(), dataBean);
						}
						if (Prop.addDataConstant && cellConstant != null && cellConstant.getStringCellValue().equals(Constants.constante)) {
							if (cellConstantValue != null) {
								dataBean.setConstant(cellConstantValue.getStringCellValue() + "");
								configBean.getListDataConstant().add(dataBean);
							}
						}
						if (cellRelationKos != null) {
							if (Utils.v(cellRelationKos.getStringCellValue())) {
								dataBean.setRelationKos(cellRelationKos.getStringCellValue());
								dataWithSkosHierarchical.add(dataBean);
							}
						}
						if (Utils.v(cellKosNameNormalized) && Utils.v(cellKosNameNormalized.getStringCellValue())) {
							dataBean.setKosNameNormalized(cellKosNameNormalized.getStringCellValue());
						} else {
							dataBean.setKosNameNormalized(dataBean.getNameNormalized());
						}

						if (Utils.v(cellKosName) && Utils.v(cellKosName.getStringCellValue())) {
							dataBean.setKosName(cellKosName.getStringCellValue());
						} else {
							dataBean.setKosName(dataBean.getName());
						}

						columnReaded++;
					}
				} finally {
                    columnSpan.end();
                }
			}

		} catch (Exception e) {
			readXlsxFileSpan.recordException(e);
		} finally {
			readXlsxFileSpan.end();
		}

		log.debug("End readXlsxFile");

	}

	private HashMap<String, SkosBean> readMappingFile(String skosPath) {
		log.debug("Init readSkosFile");

		Span readMappingFileSpan = tracer.spanBuilder("Read SKOS Mapping File: " + skosPath)
                                     .setSpanKind(SpanKind.INTERNAL)
                                     .startSpan();
		readMappingFileSpan.setAttribute("file.name", skosPath);
    	readMappingFileSpan.setAttribute("config.directory", configDirectoryString);

		HashMap<String, SkosBean> mapSkos = new HashMap<String, SkosBean>();
		File skosMappingg = new File(configDirectoryString + File.separator + skosPath);

		try (Scope scopeReadMappingFileSpan= readMappingFileSpan.makeCurrent()) {
			
			InputStream inp = null;
			Workbook wb = null;
			try {
				inp = new FileInputStream(skosMappingg);
				wb = WorkbookFactory.create(inp);
			} catch (FileNotFoundException e) {
				log.error(e.getMessage());
				readMappingFileSpan.recordException(e);
			} catch (InvalidFormatException e) {
				log.error(e.getMessage());
				readMappingFileSpan.recordException(e);
			} catch (IOException e) {
				log.error(e.getMessage());
				readMappingFileSpan.recordException(e);
			}
			if (wb != null) {
				Sheet sheet = wb.getSheetAt(0);

				for (int i = 0; i < sheet.getPhysicalNumberOfRows(); i++) {

					Row row = sheet.getRow(i);
					Cell cellId = row.getCell(0);
					Cell cellUri = row.getCell(1);

					if (cellId == null || cellUri == null) {
						continue;
					}

					// Span rowSpan = tracer.spanBuilder("Process SKOS Row " + i)
					// 	.setParent(Context.current().with(readMappingFileSpan))
					// 	.setSpanKind(SpanKind.INTERNAL)
					// 	.startSpan();

					// try (Scope rowScope = rowSpan.makeCurrent()) {

						SkosBean skosBean = new SkosBean();
						SkosBean skosBeanExtra = new SkosBean();
						String idCell = "";
						if (cellId.getCellType() == 0) {
							Double d = new Double(cellId.getNumericCellValue());
							idCell = d.intValue() + "";
						} else {
							idCell = cellId.getStringCellValue();
						}
						skosBean.setLabel(idCell);
						idCell = Utils.urlify(idCell);
						skosBean.setId(idCell);

						// rowSpan.setAttribute("skos.id", skosBean.getId());
                    	// rowSpan.setAttribute("skos.label", skosBean.getLabel());

						String uriCell = "";
						if (cellUri.getCellType() == 0) {
							Double d = new Double(cellUri.getNumericCellValue());
							uriCell = d.intValue() + "";
						} else {
							uriCell = cellUri.getStringCellValue();
							String id = uriCell.substring(uriCell.lastIndexOf("/") + 1, uriCell.length());
							if (!idCell.equals(id)) {
								skosBeanExtra.setId(id);
								skosBeanExtra.setLabel(id);
								skosBeanExtra.setURI(uriCell);
								mapSkos.put(id, skosBeanExtra);
							}
						}
						skosBean.setURI(uriCell);
						mapSkos.put(idCell, skosBean);

						// rowSpan.setAttribute("skos.uri", skosBean.getURI());

					// } finally {
					// 	rowSpan.end();
					// }
				}
			}

		} catch (Exception e) {
			readMappingFileSpan.recordException(e);
		} finally {
			readMappingFileSpan.end();
		}

		log.debug("End readSkosFile");
		return mapSkos;
	}

	private HashMap<String, SkosBean> readMappingFileCSV(String skosPath) {
		log.debug("Init readMappingFileCSV");
		HashMap<String, SkosBean> mapSkos = new HashMap<String, SkosBean>();

		Span readMappingFileCSVSpan = tracer.spanBuilder("Read SKOS Mapping CSV File: " + skosPath)
			.setSpanKind(SpanKind.INTERNAL)
			.startSpan();
		readMappingFileCSVSpan.setAttribute("file.name", skosPath);
    	readMappingFileCSVSpan.setAttribute("config.directory", configDirectoryString);

		if (skosPath.endsWith("xlsx"))
			skosPath = skosPath.replace("xlsx", "csv");
		File skosMappingg = new File(configDirectoryString + File.separator + skosPath);
		List<String> csvLines;

		try (Scope scopeReadMappingFileCSVSpan = readMappingFileCSVSpan.makeCurrent()){
			csvLines = FileUtils.readLines(skosMappingg, "UTF-8");

			for (String line : csvLines) {

				Span rowSpan = tracer.spanBuilder("Process SKOS CSV Row")
					.setSpanKind(SpanKind.INTERNAL)
					.startSpan();

				try (Scope rowScope = rowSpan.makeCurrent()) {
					String[] cells = line.split("\",\"");
					String cellId = removeStartEndCaracter(cells[0]);
					String cellUri = removeStartEndCaracter(cells[1]);

					SkosBean skosBean = new SkosBean();
					SkosBean skosBeanExtra = new SkosBean();
					skosBean.setLabel(cellId);
					cellId = Utils.urlify(cellId);
					skosBean.setId(cellId);

					String id = cellUri.substring(cellUri.lastIndexOf("/") + 1, cellUri.length());
					if (!cellId.equals(id)) {
						skosBeanExtra.setId(id);
						skosBeanExtra.setLabel(id);
						skosBeanExtra.setURI(cellUri);
						mapSkos.put(id, skosBeanExtra);
					}
					skosBean.setURI(cellUri);
					mapSkos.put(cellId, skosBean);

					rowSpan.setAttribute("skos.id", skosBean.getId());
                	rowSpan.setAttribute("skos.label", skosBean.getLabel());
					rowSpan.setAttribute("skos.uri", skosBean.getURI());

				} finally {
					rowSpan.end();
				}
			}
		} catch (IOException e) {
			log.error("Error reading CSV for SKOS mapping", e);
			readMappingFileCSVSpan.recordException(e);
		} finally {
			readMappingFileCSVSpan.end();
		}

		log.debug("End readMappingFileCSV");
		return mapSkos;
	}

	public void writeSkosTTL() {
		log.info("Init to create skos");
		Span writeSkosTTLSpan = tracer.spanBuilder("Write SKOS TTL File")
			.setSpanKind(SpanKind.INTERNAL)
			.startSpan();

		File kosFile = new File(outputDirectoryString + File.separator + "DatosTTL" + File.separator + "codelists" + File.separator + "kos.ttl");
		writeSkosTTLSpan.setAttribute("output.file", kosFile.getAbsolutePath());

		StringBuffer resultIni = new StringBuffer();
		StringBuffer resultFin = new StringBuffer();
		ArrayList<String> kosCreated = new ArrayList<String>();
		resultIni.append(TransformToRDF.addPrefix());

		writeSkosTTLSpan.setAttribute("total.skos.entries", dataWithSkos.size());

		try (Scope scopeWriteSkosTTLSpan = writeSkosTTLSpan.makeCurrent()) {

			for (Iterator<DataBean> itDataBean = dataWithSkos.iterator(); itDataBean.hasNext();) {

				DataBean dataBean = itDataBean.next();

				Span dataBeanSpan = tracer.spanBuilder("Process SKOS DataBean: " + dataBean.getKosNameNormalized())
					.setSpanKind(SpanKind.INTERNAL)
					.startSpan();
				try (Scope dataScope = dataBeanSpan.makeCurrent()) {

					dataBeanSpan.setAttribute("skos.id", dataBean.getKosNameNormalized());
                	dataBeanSpan.setAttribute("skos.name", dataBean.getKosName());
				
					if (dataBean != null && dataBean.isWriteSkos() && !kosCreated.contains(dataBean.getNameNormalized()) && dataBean.getMapSkos().size() > 0) {
						String suject = Prop.host + "/" + Prop.kosName + "/" + Prop.datasetName + "/" + dataBean.getKosNameNormalized();
						resultIni.append("<" + suject + "> " + "a skos:ConceptScheme;\n");
						resultIni.append("\tskos:notation \"" + dataBean.getKosNameNormalized() + "\";\n");
						resultIni.append("\trdfs:label \"" + dataBean.getKosName() + "\";\n");

						for (Iterator<String> iterator3 = dataBean.getMapSkos().keySet().iterator(); iterator3.hasNext();) {
							String keySkos = iterator3.next();
							SkosBean skosBean = dataBean.getMapSkos().get(keySkos);
							if (skosBean != null) {
								String sujectKos = suject + "/" + Utils.urlify(skosBean.getId());
								dataBeanSpan.setAttribute("skos.subconcepts.count", skosBean.getSons().size());

								if (skosBean.getParent() == null) {
									resultIni.append("\tskos:hasTopConcept <" + sujectKos + ">");
									if (iterator3.hasNext()) {
										resultIni.append(";\n");
									} else {
										resultIni.append(".\n");
									}
								}
								resultFin.append("<" + sujectKos + "> a skos:Concept;\n");
								resultFin.append("\tskos:inScheme <" + suject + ">;\n");
								String label = skosBean.getId();
								if (skosBean.getLabel() != null && !skosBean.getLabel().equals(""))
									label = skosBean.getLabel();
								resultFin.append("\tskos:notation \"" + skosBean.getId() + "\";\n");
								resultFin.append("\tskos:prefLabel \"" + Utils.prefLabelClean(label) + "\"");
								if (skosBean.getParent() != null) {
									resultFin.append(";\n");
									resultFin.append("\tskos:broader <" + suject + "/" + skosBean.getParent().getId() + ">");
								}
								if (skosBean.getSons().size() > 0) {
									resultFin.append(";\n");
									for (Iterator<SkosBean> itSons = skosBean.getSons().iterator(); itSons.hasNext();) {
										SkosBean son = itSons.next();
										resultFin.append("\tskos:narrower <" + suject + "/" + son.getId() + ">");
										if (itSons.hasNext()) {
											resultFin.append(";\n");
										} else {
											resultFin.append(".\n");
										}
									}
								} else {
									resultFin.append(".\n");
								}

								resultFin.append("\n");
							}
						}
						resultIni.append("\n");
						resultIni.append(resultFin);
						kosCreated.add(dataBean.getNameNormalized());
					}
					Utils.stringToFileAppend(resultIni.toString(), kosFile);
					resultIni.setLength(0);
					resultFin.setLength(0);

				} finally {
					dataBeanSpan.end();
				}
			}

		} finally {
			writeSkosTTLSpan.end();
		}

		log.info("end to create skos");
		log.debug("End createSkos");
	}

	public List<String> writeInformationTTL() {
		log.debug("Init extractInformation");

		Span writeInformationSpan = tracer.spanBuilder("Write Information TTL")
            .setSpanKind(SpanKind.INTERNAL)
            .startSpan();

		List<String> result = new ArrayList<>();
		File inputDirectoryFile = new File(inputDirectoryString);
		File propertiesFile = new File(outputDirectoryString + File.separator + "DatosTTL" + File.separator + "codelists" + File.separator + "properties.ttl");
		File dsdFile = new File(outputDirectoryString + File.separator + "DatosTTL" + File.separator + "dataStructures" + File.separator + "dsd.ttl");
		File errorReportFile = new File("errorReport.txt");

		try (Scope scopeWriteInformationSpan = writeInformationSpan.makeCurrent()) {

			TransformToRDF.propertiesContent.append(TransformToRDF.addPrefix());
			Utils.stringToFileAppend(TransformToRDF.addPrefix().toString(), dsdFile);

			Collection<File> listCSV = FileUtils.listFiles(inputDirectoryFile, extensions, true);
			int numfile = 1;
			for (File file : listCSV) {

				Span fileProcessingSpan = tracer.spanBuilder("Process CSV File - " + file.getName())
                    .setSpanKind(SpanKind.INTERNAL)
                    .startSpan();

				try (Scope fileScope = fileProcessingSpan.makeCurrent()){
					String fileName = "";
					String fileLetter = "";
					if (file.getName().endsWith("A.csv")) {
						fileName = file.getName().substring(0, file.getName().length() - 5);
						fileLetter = file.getName().substring(file.getName().length() - 5, file.getName().length() - 4);
					} else {
						fileName = file.getName().substring(0, file.getName().length() - 6);
						fileLetter = file.getName().substring(file.getName().length() - 6, file.getName().length() - 4);
					}

					fileProcessingSpan.setAttribute("file.name", file.getName());
					fileProcessingSpan.setAttribute("file.size", FileUtils.sizeOf(file));
					fileProcessingSpan.setAttribute("file.number", numfile);
					fileProcessingSpan.setAttribute("file.total", listCSV.size());

					if (!filesNotRDF.contains(fileName + fileLetter)) {
						ConfigBean configBean = mapconfig.get(fileName);
						if (configBean != null) {
							File outputDirectoryFile = new File(outputDirectoryString + File.separator + "DatosTTL" + File.separator + "informes" + File.separator + fileName + fileLetter + ".ttl");
							log.info("Init file " + fileName + fileLetter + ". Size " + FileUtils.sizeOf(file) + " " + numfile + "/" + listCSV.size());
							List<String> csvLines = FileUtils.readLines(file, "UTF-8");
							String description = idDescription.get(fileName + fileLetter);

							if (description != null) {
								fileProcessingSpan.setAttribute("file.description", description);
							}
							fileProcessingSpan.addEvent("Start transformation");

							TransformToRDF transformToRDF = new TransformToRDF(csvLines, outputDirectoryFile, propertiesFile, dsdFile, errorReportFile, configBean, specsTtlFileString);
							transformToRDF.initTransformation(fileName + fileLetter, numfile, fileName, dsdList, propertiesList, description);

							

							log.info("End file " + outputDirectoryFile.getName() + " " + numfile + "/" + listCSV.size());
							result.add(fileName + fileLetter);
							fileProcessingSpan.addEvent("End transformation");
						} else {
							log.error("Error al extraer la configuración de " + fileName);
							fileProcessingSpan.setAttribute("error", true);
						}
						numfile++;
					}
				} catch (Exception e) {
					log.error("Error al extraer la información ", e);
					fileProcessingSpan.setAttribute("error", true);
					fileProcessingSpan.setAttribute("error.message", e.getMessage());
				} finally {
					fileProcessingSpan.end();
				}
			}
			TransformToRDF transformToRDF = new TransformToRDF(propertiesFile, dsdFile, errorReportFile, specsTtlFileString);
			transformToRDF.generateCommonData(mapconfig, idDescription);

		} catch (Exception e) {
			writeInformationSpan.setAttribute("error", true);
			writeInformationSpan.setAttribute("error.message", e.getMessage());
			log.error("Error en writeInformationTTL ", e);
		} finally {
			writeInformationSpan.end();
			log.debug("End extractInformation");
		}

		log.debug("End extractInformation");
		return result;
	}
	
	
	public void generateCommonData(){
		Span generateCommonDataSpan = tracer.spanBuilder("Generate Common Data")
            .setSpanKind(SpanKind.INTERNAL)
            .startSpan();

    	try (Scope scopeGenerateCommonDataSpan = generateCommonDataSpan.makeCurrent()) {
			File propertiesFile = new File(outputDirectoryString + File.separator + "DatosTTL" + File.separator + "codelists" + File.separator + "properties.ttl");
			File dsdFile = new File(outputDirectoryString + File.separator + "DatosTTL" + File.separator + "dataStructures" + File.separator + "dsd.ttl");
			File errorReportFile = new File("errorReport.txt");
			TransformToRDF transformToRDF = new TransformToRDF(propertiesFile, dsdFile, errorReportFile, specsTtlFileString);
			transformToRDF.generateCommonData(mapconfig, idDescription);

			generateCommonDataSpan.setAttribute("propertiesFile.path", propertiesFile.getPath());
			generateCommonDataSpan.setAttribute("dsdFile.path", dsdFile.getPath());
			generateCommonDataSpan.setAttribute("errorReportFile.path", errorReportFile.getPath());
			generateCommonDataSpan.addEvent("Common data generated successfully");

		} catch (Exception e) {
			log.error("Error in generateCommonData", e);
			generateCommonDataSpan.setAttribute("error", true);
			generateCommonDataSpan.setAttribute("error.message", e.getMessage());
		} finally {
			generateCommonDataSpan.end();
			log.debug("End generateCommonData");
		}
	}
	
	public void backup() {

		log.debug("Init backup");
		log.info("Comienza a hacerse el backup");

		Span backupSpan = tracer.spanBuilder("Backup Process")
            .setSpanKind(SpanKind.INTERNAL)
            .startSpan();

    try (Scope scopeBackupSpan = backupSpan.makeCurrent()) {

			File outputDirectoryFile = new File(outputDirectoryString);
			if (outputDirectoryFile.exists()) {
				SimpleDateFormat formatFullDate = new SimpleDateFormat("yyyyMMdd");
				String copy = outputDirectoryString + "_" + formatFullDate.format(new Date());
				File copyDirectoryFile = new File(copy);
				int aux = 1;

				backupSpan.setAttribute("outputDirectory.path", outputDirectoryFile.getPath());
            	backupSpan.setAttribute("copyDirectory.initialPath", copyDirectoryFile.getPath());

				while (copyDirectoryFile.exists()) {
					copyDirectoryFile = new File(copy + "_" + aux++);
				}

				backupSpan.setAttribute("copyDirectory.finalPath", copyDirectoryFile.getPath());

				try {
					FileUtils.moveDirectoryToDirectory(outputDirectoryFile, copyDirectoryFile, true);
					backupSpan.addEvent("Backup completed successfully");
				} catch (IOException e) {
					log.error("Error haciendo backup", e);
					backupSpan.setAttribute("error", true);
                	backupSpan.setAttribute("error.message", e.getMessage());
				}
			}
			log.info("Finaliza de hacerse el backup");
		} finally {
			backupSpan.end();
			log.debug("End backup");
		}
	}

	public void delete() {
		Span deleteSpan = tracer.spanBuilder("Delete Process")
				.setSpanKind(SpanKind.INTERNAL)
				.startSpan();

		try (Scope scopeDeleteSpan = deleteSpan.makeCurrent()) {
			log.info("Comienza a hacerse el delete");

			File outputDirectoryFile = new File(outputDirectoryString);
			deleteSpan.setAttribute("outputDirectory.path", outputDirectoryFile.getPath());

			try {
				if (outputDirectoryFile.exists()) {
					FileUtils.deleteDirectory(outputDirectoryFile);
					deleteSpan.addEvent("Directory deleted successfully");
				} else {
					deleteSpan.addEvent("Directory does not exist, no action taken");
				}
			} catch (IOException e) {
				log.error("Error borrando los ficheros viejos", e);
				deleteSpan.setAttribute("error", true);
				deleteSpan.setAttribute("error.message", e.getMessage());
			}

			log.info("Finaliza de hacerse el delete");
		} finally {
			deleteSpan.end();
			log.debug("End delete");
		}
	}

	public void zipFiles() {

		Span zipFilesSpan = tracer.spanBuilder("Zip Files Process")
            .setSpanKind(SpanKind.INTERNAL)
            .startSpan();

		try (Scope scopeZipFilesSpan = zipFilesSpan.makeCurrent()) {
			
			File kosFileDump = new File(outputDirectoryString + File.separator + "DatosTTL" + File.separator + "codelists" + File.separator + "kos.ttl");
			File propertiesFileDump = new File(outputDirectoryString + File.separator + "DatosTTL" + File.separator + "codelists" + File.separator + "properties.ttl");
			File dsdFileDump = new File(outputDirectoryString + File.separator + "DatosTTL" + File.separator + "dataStructures" + File.separator + "dsd.ttl");

			String pathzip = outputDirectoryString + File.separator + "zip" + File.separator + "commonData";
			File kosFileZip = new File(pathzip + File.separator + "kos.ttl");
			File propertiesFileZip = new File(pathzip + File.separator + "properties.ttl");
			File dsdFileZip = new File(pathzip + File.separator + "dsd.ttl");

			try {
				if (kosFileDump.exists())
					FileUtils.copyFile(kosFileDump, kosFileZip);
					zipFilesSpan.addEvent("Copied kos.ttl file");
				if (propertiesFileDump.exists())
					FileUtils.copyFile(propertiesFileDump, propertiesFileZip);
					 zipFilesSpan.addEvent("Copied properties.ttl file");
				if (dsdFileDump.exists())
					FileUtils.copyFile(dsdFileDump, dsdFileZip);
					zipFilesSpan.addEvent("Copied dsd.ttl file");
				Utils.zipFolders(pathzip, true);
				zipFilesSpan.addEvent("Zipped commonData folder");

				File informes = new File(outputDirectoryString + File.separator + "DatosTTL" + File.separator + "informes");
				if (!informes.exists()){
					zipFilesSpan.addEvent("No informes folder found, skipping individual file compression");
					return;
				}
				
				Collection<File> listTTL = FileUtils.listFiles(informes, extensionsZip, true);
				for (File ttlFileDump : listTTL) {

					String nameFile = ttlFileDump.getName().substring(0, ttlFileDump.getName().length() - 4);
					pathzip = outputDirectoryString + File.separator + "zip" + File.separator + nameFile;
					File ttlFileZip = new File(pathzip + File.separator + ttlFileDump.getName());
					FileUtils.copyFile(ttlFileDump, ttlFileZip);
					zipFilesSpan.addEvent("Copied file " + ttlFileDump.getName());
					Utils.zipFolders(pathzip, true);
					zipFilesSpan.addEvent("Zipped folder for file " + ttlFileDump.getName());

				}

			} catch (IOException e) {
				log.error("Error comprimiendo los ficheros generados", e);
				zipFilesSpan.setAttribute("error", true);
				zipFilesSpan.setAttribute("error.message", e.getMessage());
			}
		} finally {
			zipFilesSpan.end();
		}
	}

	public HashSet<String> getFilesNotRDF() {
		return filesNotRDF;
	}

	public void setFilesNotRDF(HashSet<String> filesNotRDF) {
		this.filesNotRDF = filesNotRDF;
	}

	public HashMap<String, ConfigBean> getMapconfig() {
		return mapconfig;
	}

	public void setMapconfig(HashMap<String, ConfigBean> mapconfig) {
		this.mapconfig = mapconfig;
	}

	public static void main(String[] args) {
		if ((log == null) || (log.getLevel() == null))
			PropertyConfigurator.configure("log4j.properties");
		if (args.length == 6) {
			log.info("Start process");
			Prop.loadConf();
			GenerateRDF app = new GenerateRDF(args[1], args[2], args[3], args[4], args[5]);
			app.delete();
			app.readConfig(null);
			app.writeInformationTTL();
			app.writeSkosTTL();
			app.zipFiles();
			log.info("Finish process");
		} else {
			log.info("Se deben de pasar dos parámetros: ");
			log.info("La cadena de texto data ");
			log.info("\tEl directorio donde están los archivos de entrada");
			log.info("\tEl directorio donde se van a escribir los archivos ttl");
			log.info("\tEl directorio donde están los excel de configuación");
		}

	}
}
