package com.localidata.extract;

import java.io.File;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.localidata.generic.Constants;
import com.localidata.generic.Prop;
import com.localidata.util.Cookies;
import com.localidata.util.Jdbcconnection;
import com.localidata.util.Utils;
import com.localidata.util.OpenTelemetryConfig;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;

/**
 * @author Localidata
 */
public class GenerateCSV {
	private final static Logger log = Logger.getLogger(GenerateCSV.class);
	private static final Tracer tracer = OpenTelemetryConfig.getTracer();

	private String urlsFileString = "";
	private String outputFilesDirectoryString = "";
	private HashMap<String, String> hashCodeOld = new HashMap<>();
	private HashMap<String, String> hashCodeNew = new HashMap<>();
	private HashMap<String, String> idDescription = new HashMap<>();
	private List<String> changes = new ArrayList<String>();
	private List<String> news = new ArrayList<String>();

	public GenerateCSV(String urls, String outputFiles) {

		Span generateCSVSpan = tracer.spanBuilder("Generate CSV builder")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();

		try (Scope scopeGenerateCSVSpan = generateCSVSpan.makeCurrent()){
			urlsFileString = urls;
			outputFilesDirectoryString = outputFiles;
			log.info("Nos conectamos a la base de datos para generar los ficheros");
			log.info("Generando el fichero InformesEstadisticaLocal-URLs.csv");

			generateCSVSpan.setAttribute(AttributeKey.stringKey("urlsFileString"), urlsFileString);
			generateCSVSpan.setAttribute(AttributeKey.stringKey("outputFilesDirectoryString"), outputFilesDirectoryString);

			Jdbcconnection.main(null);
		} catch (Exception e) {
			generateCSVSpan.setAttribute("error", true);
            generateCSVSpan.setAttribute("error.message", e.getMessage());
			log.error("Error generando informe bbdd iaest",e);
		}finally {
            generateCSVSpan.end();
			log.info("Fin de la generación del fichero InformesEstadisticaLocal-URLs.csv");
        }
		
	}

	public void extractFiles() {
		Span extractFilesCSVSpan = tracer.spanBuilder("Extract files in GenerateCSV")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();

		try (Scope scopeExtractFilesCSVSpan = extractFilesCSVSpan.makeCurrent()){
				
			log.info("Inicio extractFilesWithChanges");
			extractHashCode();
			List<String> all = new ArrayList<>();
			all.add("cabecera");
			Cookies cookies = new Cookies();
			File urlsFile = new File(urlsFileString);
			HashMap<String[], Integer> numErrorFiles = new HashMap<>();
			HashMap<String[], String> errorFiles = new HashMap<>();
			String[] valores = null;
			String content = null;

			Map<String, String> headers = new HashMap<String, String>();
			headers.put("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.86 Safari/537.36");
			headers.put("Cookie", "sawU=" + Prop.sawUiAragonBiAragon + "; __utma=" + Prop.utmaBiAragon + "; __utmc=" + Prop.utmcBiAragon + "; __utmz=" + Prop.utmzBiAragon);			
			headers.put("content-type", "text/csv; charset=ISO-8859-1");			
			Utils.processURLGet(Prop.urlBiAragon + Prop.initialDataCube + "&Action=Download&Options=df" , "", headers, cookies, "ISO-8859-1");

			List<String> csvLines = FileUtils.readLines(urlsFile, "UTF-8");

			//ESTABLECEMOS COOKIES EN EL HEADER
			Map mapaCookiesAragon = (Map)cookies.getStore().get("aragon.es");

			for(Object cabecera : mapaCookiesAragon.keySet()) {				
				if(cabecera.toString().contains("_WL_AUTHCOOKIE_JSESSIONID")) {
					String lineaCookies = headers.get("Cookie");
					
					Map sesion = (Map)mapaCookiesAragon.get("_WL_AUTHCOOKIE_JSESSIONID");
					String authCookieSesion = sesion.get("_WL_AUTHCOOKIE_JSESSIONID").toString();
					lineaCookies += "; " + cabecera + "=" + authCookieSesion;	
					
					headers.replace("Cookie", lineaCookies);
				}
				else if(cabecera.toString().contains("JSESSIONID")){							
					String lineaCookies = headers.get("Cookie");
					
					Map sesionID = (Map)mapaCookiesAragon.get("JSESSIONID");
					String cookieSesionID = sesionID.get("JSESSIONID").toString();
					lineaCookies += "; " + "JSESSIONID" + "=" + cookieSesionID;
					
					headers.replace("Cookie", lineaCookies);
				}					
				else if(cabecera.toString().contains("ORA_BIPS_NQID")){							
					String lineaCookies = headers.get("Cookie");
					
					Map sesionID = (Map)mapaCookiesAragon.get("ORA_BIPS_NQID");
					String cookieSesionID = sesionID.get("ORA_BIPS_NQID").toString();
					lineaCookies += "; " + "ORA_BIPS_NQID" + "=" + cookieSesionID;
					
					headers.replace("Cookie", lineaCookies);
				}	
				else if(cabecera.toString().contains("ORA_BIPS_LBINFO")){							
					String lineaCookies = headers.get("Cookie");
					
					Map sesionID = (Map)mapaCookiesAragon.get("ORA_BIPS_LBINFO");
					String cookieSesionID = sesionID.get("ORA_BIPS_LBINFO").toString();
					lineaCookies += "; " + "ORA_BIPS_LBINFO" + "=" + cookieSesionID;
					
					headers.replace("Cookie", lineaCookies);
				}	
			}

			for (int h = 1; h < csvLines.size(); h++) {

				// Span downloadFilesSpan = tracer.spanBuilder("Download files in extractFiles")
				// 	.setSpanKind(SpanKind.INTERNAL)
				// 	.startSpan();
				
				boolean procesar = true;
				try{		
				// try (Scope scopeDownloadFilesSpan = downloadFilesSpan.makeCurrent()){		
					cookies = new Cookies();
					String line = csvLines.get(h);
					valores = line.split(",");
					valores[0] = valores[0].replaceAll("\"", "");
					valores[1] = valores[1].replaceAll("\"", "");
					valores[2] = valores[2].replaceAll("\"", "");
											
					                // **NUEVO: Filtro para procesar solo un cubo específico**
									if (!valores[1].equals("03-030040TP")) {
										log.info("Saltando cubo " + valores[1] + " (no coincide con el filtro: " + Prop.specificCubeId + ")");
										continue;
									}
									
									log.info("Procesando cubo: " + valores[1]);
									idDescription.put(valores[1], valores[2]);
									
					content = Utils.processURLGet(Prop.urlBiAragon + valores[0] + "&Action=Download&Options=df", "", headers, cookies, "ISO-8859-1");
					
					// Calculamos el numero de bytes que ocupa el string en local
					int numBytesOcupa = content.length() / 2;					
					
					if(Prop.limitCubeSizeActive) {
						if(numBytesOcupa > Prop.limitCubeSize) {
							procesar = false;
							log.info("Se ha superado el limite establecido por parametro para procesar el cubo " + valores[0] + ":" + numBytesOcupa + " bytes");
							// downloadFilesSpan.setAttribute(AttributeKey.stringKey("cubo"), valores[0]);
							// downloadFilesSpan.setAttribute(AttributeKey.booleanKey("download"), false);
							log.error("Error al descargar " + valores[1]);
						}
						// }else{
						// 	downloadFilesSpan.setAttribute(AttributeKey.stringKey("cubo"), valores[0]);
						// 	downloadFilesSpan.setAttribute(AttributeKey.booleanKey("download"), true);
						// }
					}
					
					if (Utils.v(content) && procesar) {
						extractFilesCSVSpan.addEvent("Clean and transform");
						content = cleanAndTransform(content);
						
						String hash = Utils.generateHash(content);
						processContentFile(all, numErrorFiles, errorFiles, valores, content, hash);

						// downloadFilesSpan.setAttribute(AttributeKey.stringKey("hash"), hash);
					}

				}catch (Exception e) {
					numErrorFiles.put(valores, new Integer(0));
					errorFiles.put(valores, content);
					String valor = valores.length>0 ? valores[1] : "";
					log.error("Error al descargar " + valor, e);
					// downloadFilesSpan.setAttribute("error", true);
					// downloadFilesSpan.setAttribute("error.message", e.getMessage());
					
				}
				// finally {
				// 	downloadFilesSpan.end();
				// }
				
			}
			
			int j = 0;
			int totalElements = numErrorFiles.size();

			Iterator<String[]> iterator = numErrorFiles.keySet().iterator();
			while (j < totalElements) {

				// Span retryDownloadFilesSpan = tracer.spanBuilder("Retry download files in extractFiles")
				// 	.setSpanKind(SpanKind.INTERNAL)
				// 	.startSpan();

				try{
				// try (Scope scopeRetryDownloadFilesSpan = retryDownloadFilesSpan.makeCurrent()){

					valores = iterator.next();
					Integer numErrors = numErrorFiles.get(valores);
					boolean sucess = false;

					// retryDownloadFilesSpan.setAttribute("csv_file_name", valores[1]);
        			// retryDownloadFilesSpan.setAttribute("current_attempts", numErrors);

					while (numErrors < 5 && numErrors != -1 && sucess ) {
						//log.info("Intento "+numErrors+" del csv "+valores[0]);
	
						content = Utils.processURLGet(Prop.urlBiAragon + valores[0] + "&Action=Download&Options=df" , "", headers, cookies, "ISO-8859-1");
						if (Utils.v(content)) {
							extractFilesCSVSpan.addEvent("Clean and transform");
							content = cleanAndTransform(content);
							if (!content.contains(Constants.errorDoctypeHtml1) && !content.contains(Constants.errorHtml) && !content.contains(Constants.errorDoctypeHtml2) && !content.contains(Constants.errorDiv) && !content.contains(Constants.errorNingunaFila)) {
								// retryDownloadFilesSpan.addEvent("File downloaded successfully");
								Utils.stringToFile(content, new File(outputFilesDirectoryString + File.separator + valores[1] + ".csv"));
								String hash = Utils.generateHash(content);
								processContentFile(all, numErrorFiles, errorFiles, valores, content, hash);
								numErrorFiles.put(valores, new Integer(-1));
								errorFiles.remove(valores);
								sucess=true;

								// retryDownloadFilesSpan.setAttribute("download_status", "success");
                    			// retryDownloadFilesSpan.setAttribute("file_hash", hash);
							} else if (!content.contains(Constants.errorExcedidoN) && !content.contains(Constants.errorRutaNoEncontrada) && !content.contains(Constants.errorNingunaFila)) {
								log.error("Informe " + valores[1] + " imposible de descargar intento " + (numErrors + 1));
								numErrorFiles.put(valores, new Integer(-1));

								// retryDownloadFilesSpan.setAttribute("download_status", "failed");
								// retryDownloadFilesSpan.addEvent("File download failed", Attributes.of(
								// 	AttributeKey.stringKey("reason"), "Invalid content detected"
								// ));
							} else {
								log.error("Informe " + valores[1] + " imposible de descargar intento " + (numErrors + 1));
								numErrorFiles.put(valores, ++numErrors);

								// retryDownloadFilesSpan.setAttribute("download_status", "retrying");
								// retryDownloadFilesSpan.addEvent("Retrying download", Attributes.of(
								// 	AttributeKey.longKey("next_attempt"), numErrors.longValue()
								// ));
							}
						}
						Thread.sleep(1000);
					}
					if (!iterator.hasNext()) {
						iterator = numErrorFiles.keySet().iterator();
						j++;
					}
				}catch (InterruptedException e) {
					log.error("Error al descargar " + valores[1], e);
					throw e; // Lanzar nuevamente la InterruptedException
				} catch (Exception e2) {
					log.error("Error al descargar " + valores[1], e2);
				}
				// finally {
				// 	retryDownloadFilesSpan.end();
				// }
			}

			for (String[] val : errorFiles.keySet()) {
				String cont = errorFiles.get(valores);
				informeErrores(val[1], cont);
			}

		} catch (Exception e) {
			extractFilesCSVSpan.setAttribute("error", true);
            extractFilesCSVSpan.setAttribute("error.message", e.getMessage());
			
		}finally {
            extractFilesCSVSpan.end();
        }

		log.info("End extractFilesWithChanges");
	}

	private void processContentFile(List<String> all, HashMap<String[], Integer> numErrorFiles, HashMap<String[], String> errorFiles, String[] valores, String content, String hash) throws Exception {
		Span processContentFileSpan = tracer.spanBuilder("Process content file in GenerateCSV")
			.setSpanKind(SpanKind.INTERNAL)
			.startSpan();

		try (Scope scopeProcessContentFileSpan = processContentFileSpan.makeCurrent()){

			processContentFileSpan.setAttribute("file_name", valores[1]);
        	processContentFileSpan.setAttribute("hash", hash);
		
			boolean safeFile = false;
			//log.debug("processContentFile hash "+hash);
			if (hashCodeOld.get(valores[1]) == null) {
				processContentFileSpan.addEvent("New data cube found: " + valores[1]);
				news.add(valores[1]);
				all.add(valores[0] + "," + valores[1]);
				safeFile = true;
			} else if (!hashCodeOld.get(valores[1]).equals(hash)) {
				processContentFileSpan.addEvent("Changes detected in data cube: " + valores[1]);
				changes.add(valores[1]);
				all.add(valores[0] + "," + valores[1]);
				safeFile = true;
			} else {
				processContentFileSpan.addEvent("No changes detected in data cube: " + valores[1]);
			}
			if (safeFile) {
				
				if (!content.contains(Constants.errorDoctypeHtml1) && !content.contains(Constants.errorHtml) && !content.contains(Constants.errorDoctypeHtml2) && !content.contains(Constants.errorDiv) && !content.contains(Constants.errorNingunaFila)) {
					
					processContentFileSpan.addEvent("File content validated successfully");
					Utils.stringToFile(content, new File(outputFilesDirectoryString + File.separator + valores[1] + ".csv"));
					hashCodeNew.put(valores[1], hash);
					processContentFileSpan.setAttribute("status", "file_downloaded: "  + valores[1]);

				} else if (!content.contains(Constants.errorExcedidoN) && !content.contains(Constants.errorRutaNoEncontrada) && !content.contains(Constants.errorNingunaFila)) {
					processContentFileSpan.addEvent("File download failed - validation error");
					numErrorFiles.put(valores, new Integer(0));
					errorFiles.put(valores, content);
					news.remove(valores[1]);
					changes.remove(valores[1]);
					processContentFileSpan.setAttribute("status", "file_download_failed: " + valores[1]);
                	processContentFileSpan.setAttribute("failure_reason", "Validation error in content");
				} else {
					processContentFileSpan.addEvent("Critical error during file download");
					informeErrores(valores[1], content);
					news.remove(valores[1]);
					changes.remove(valores[1]);
					processContentFileSpan.setAttribute("status", "file_download_failed: " + valores[1]);
                	processContentFileSpan.setAttribute("failure_reason", "Critical error during file download");
				}
			}
		} catch (Exception e) {
			processContentFileSpan.recordException(e);
			processContentFileSpan.setAttribute("error", true);
			processContentFileSpan.setAttribute("file", valores[1]);
			throw e;
		} finally {
			processContentFileSpan.end();
		}
	}

	private String cleanAndTransform(String content) {

		try{
			int separador = 0;
			String cadena_reemplazar = (char)separador + "";
			content = content.replace(cadena_reemplazar, ""); //REMPLAZAMOS EL CARACTER NULO (EN OCASIONES VIENE UN CARACTER NULO ENTRE CADA CARACTER DEL CONTENIDO)
			content = content.replace(",0\"", "\"");
			content = content.replace("\"", "");
			content = content.replace(new String(Character.toChars(0)), "");
			content = content.replace("ÿþ", "");
			
			return content;

		} catch (Exception e) {
			log.error(e);
			throw e;
		}
	}

	public void generateHashCode(List<String> result, List<String> list) {

		Span generateHashCodeSpan = tracer.spanBuilder("Generate hashcode in GenerateCSV")
            .setSpanKind(SpanKind.INTERNAL)
            .startSpan();
		
		try(Scope scopeGenerateHashCodeSpan = generateHashCodeSpan.makeCurrent()) {
			generateHashCodeSpan.setAttribute("result_size", result.size());
        	generateHashCodeSpan.setAttribute("list_size", list.size());

			File fileCSV = new File(String.valueOf(Prop.fileHashCSV) + "." + "csv");
			File fileXlsx = new File(String.valueOf(Prop.fileHashCSV) + "." + "xlsx");

			generateHashCodeSpan.setAttribute("csv_file_path", fileCSV.getAbsolutePath());
        	generateHashCodeSpan.setAttribute("xlsx_file_path", fileXlsx.getAbsolutePath());

			String hashCodeFile = "";
			
			for (String key : this.hashCodeOld.keySet()) {
				String hash = "";
				if (result.contains(key)) {
					generateHashCodeSpan.addEvent("Key found in result", Attributes.of(AttributeKey.stringKey("key"), key));
					hash = this.hashCodeNew.get(key);
				} else {
					generateHashCodeSpan.addEvent("Key not found in result, using old hash", Attributes.of(AttributeKey.stringKey("key"), key));
					hash = this.hashCodeOld.get(key);
				} 
				hashCodeFile = String.valueOf(hashCodeFile) + key + "," + hash + "\n";
			} 

			generateHashCodeSpan.setAttribute("initial_hashCodeNew_size", this.hashCodeNew.size());
			
			for (String key : this.hashCodeNew.keySet()) {
				String hash = "";
				if (!hashCodeFile.contains(key)) {
					generateHashCodeSpan.addEvent("New key added", Attributes.of(AttributeKey.stringKey("key"), key));
					hash = this.hashCodeNew.get(key);
					hashCodeFile = String.valueOf(hashCodeFile) + key + ",nuevo\n";
				}
			} 

			Utils.stringToFile(hashCodeFile, fileCSV);
			generateHashCodeSpan.addEvent("CSV file generated successfully");

			Utils.csvToXLSX(fileCSV, fileXlsx);
			generateHashCodeSpan.addEvent("CSV converted to XLSX successfully");

			log.info("Hashcode generado correctamente");

		} catch (Exception e) {
			generateHashCodeSpan.recordException(e);
			generateHashCodeSpan.setAttribute("error", true);
            generateHashCodeSpan.setAttribute("error.message", e.getMessage());
			log.error("Error generando fichero hashcode", e);
		}finally {
            generateHashCodeSpan.end();
        }
	}
	
	public void generateHashCodeFromBI() {
		
		File urlsFile = new File(urlsFileString);
		File hashFile = new File(Prop.fileHashCSV + "." + Constants.CSV);
		Cookies cookies = new Cookies();
		String[] valores = null;
		String content = null;
		StringBuffer result = new StringBuffer();
		HashMap<String[], Integer> numErrorFiles = new HashMap<>();
		HashMap<String[], String> errorFiles = new HashMap<>();
		try{
			Map<String, String> headers = new HashMap<String, String>();
			headers.put("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.86 Safari/537.36");
			headers.put("Cookie", "sawU=" + Prop.sawUiAragonBiAragon + "; ORA_BIPS_LBINFO=" + Prop.oraBipsLbinfoBiAragon + "; ORA_BIPS_NQID=" + Prop.oraBipsNqidBiAragon + "; __utma=" + Prop.utmaBiAragon + "; __utmc=" + Prop.utmcBiAragon + "; __utmz=" + Prop.utmzBiAragon);
			headers.put("content-type", "text/csv; charset=ISO-8859-1");
			Utils.processURLGet(Prop.urlBiAragon + Prop.initialDataCube + "&Action=Download&Options=df", "", headers, cookies, "ISO-8859-1");
			List<String> csvLines = FileUtils.readLines(urlsFile, "UTF-8");
	
			try {
				for (int h = 1; h < csvLines.size(); h++) {
					String line = csvLines.get(h);
					valores = line.split(",");
					valores[0] = valores[0].replaceAll("\"", "");
					valores[1] = valores[1].replaceAll("\"", "");
					valores[2] = valores[2].replaceAll("\"", "");
					content = Utils.processURLGet(Prop.urlBiAragon + valores[0] + "&Action=Download&Options=df" , "", headers, cookies, "ISO-8859-1");
					if (Utils.v(content) && !valores[1].equals("")) {
						// scopeExtractFilesCSVSpan.addEvent("Clean and transform");
						content = cleanAndTransform(content);
						String hash = Utils.generateHash(content);
						result.append(valores[1]+","+hash+"\n");
						//log.info(valores[1]+","+hash);
					}else{
						numErrorFiles.put(valores, new Integer(0));
						errorFiles.put(valores, content);
					}
				}
			} catch (IOException e2) {
				numErrorFiles.put(valores, new Integer(0));
				errorFiles.put(valores, content);
				//log.error("Error al descargar " + valores[1], e2);
			}
			
			int j = 0;
			int totalElements = numErrorFiles.size();
			
			try {
				Iterator<String[]> iterator = numErrorFiles.keySet().iterator();
				while (j < totalElements) {
					valores = iterator.next();
					Integer numErrors = numErrorFiles.get(valores);
					if (numErrors < 5 && numErrors != -1) {
						content = Utils.processURLGet(Prop.urlBiAragon + valores[0] + "&Action=Download&Options=df" , "", headers, cookies, "ISO-8859-1");
						if (Utils.v(content)) {
							// scopeExtractFilesCSVSpan.addEvent("Clean and transform");
							content = cleanAndTransform(content);
							if (!content.contains(Constants.errorDoctypeHtml1) && !content.contains(Constants.errorHtml) && !content.contains(Constants.errorDoctypeHtml2) && !content.contains(Constants.errorDiv)) {
								Utils.stringToFile(content, new File(outputFilesDirectoryString + File.separator + valores[1] + ".csv"));
								String hash = Utils.generateHash(content);
								result.append(valores[1]+","+hash+"\n");
								numErrorFiles.put(valores, new Integer(-1));
								errorFiles.remove(valores);
							} else if (!content.contains(Constants.errorExcedidoN) && !content.contains(Constants.errorRutaNoEncontrada)) {
								numErrorFiles.put(valores, new Integer(-1));
							} else {
								//log.error("Informe " + valores[1] + " imposible de descargar intento " + (numErrors + 1));
								numErrorFiles.put(valores, ++numErrors);
							}
						}
						Thread.sleep(1000);
					}
					if (!iterator.hasNext()) {
						iterator = numErrorFiles.keySet().iterator();
						j++;
					}
				}
			} catch (IOException e2) {
				log.error("Error al descargar " + valores[1], e2);
			}
			
			try {
				Utils.stringToFile(result.toString(), hashFile);
				//log.info("Hashcode generado correctamente");
			} catch (Exception e) {
				log.error("Error generando fichero hashcode", e);
			}
			
		}
//		catch (Exception e) {
//			log.error("Error desconocido", e);
//		}
		catch (InterruptedException e) {
			log.error("Error desconocido" + valores[1], e);
			// Reinterrumpir el hilo para mantener el estado de interrupción
			Thread.currentThread().interrupt();
		} catch (Exception e2) {
			log.error("Error desconocido " + valores[1], e2);
		}
	}

protected void informeErrores(String id, String content) {
    Span informeErroresSpan = tracer.spanBuilder("Report errors in GenerateCSV")
            .setSpanKind(SpanKind.INTERNAL)
            .startSpan();

    try (Scope scopeInformeErroresSpan = informeErroresSpan.makeCurrent()) {
        informeErroresSpan.setAttribute("file_id", id);
        if (content != null) {
            informeErroresSpan.setAttribute("content_length", content.length());

            if (content.contains("Se ha excedido el n")) {
                informeErroresSpan.addEvent("Error type: Exceeded limit");
                File file = new File(Prop.fileErrorBig);
                informeErroresSpan.setAttribute("error_file_path", file.getAbsolutePath());
                Utils.stringToFileAppend(id + ".csv" + System.lineSeparator(), file);
            } else if (content.contains("Ruta de acceso no encontrada")) {
                informeErroresSpan.addEvent("Error type: Path not found");
                File file = new File(Prop.fileErrorNotFound);
                informeErroresSpan.setAttribute("error_file_path", file.getAbsolutePath());
                Utils.stringToFileAppend(id + ".csv" + System.lineSeparator(), file);
            } else {
                informeErroresSpan.addEvent("Error type: Generic error");
                File file = new File(Prop.fileErrorGeneric);
                informeErroresSpan.setAttribute("error_file_path", file.getAbsolutePath());
                Utils.stringToFileAppend(id + ".csv" + System.lineSeparator(), file);
            }
        } else {
            informeErroresSpan.addEvent("Content is null");
            informeErroresSpan.setAttribute("error", true);
            //log.warn("Content provided to informeErrores is null for file ID: " + id);
        }
    } catch (Exception e) {
        informeErroresSpan.recordException(e);
        informeErroresSpan.setAttribute("error", true);
        informeErroresSpan.setAttribute("error.message", e.getMessage());
        log.error("Error en informeErrores para el archivo " + id, e);
    } finally {
        informeErroresSpan.end();
    }
}
	
protected void extractHashCode() {
    Span extractHashCodeSpan = tracer.spanBuilder("Extract hashcode in GenerateCSV")
            .setSpanKind(SpanKind.INTERNAL)
            .startSpan();

    try (Scope scopeExtractHashCodeSpan = extractHashCodeSpan.makeCurrent()) {
        File fileCSV = new File(Prop.fileHashCSV + "." + "csv");
        File fileXLSX = new File(Prop.fileHashCSV + "." + "xlsx");

        extractHashCodeSpan.setAttribute("csv_file_path", fileCSV.getAbsolutePath());
        extractHashCodeSpan.setAttribute("xlsx_file_path", fileXLSX.getAbsolutePath());

        try {
            extractHashCodeSpan.addEvent("Converting XLSX to CSV");
            Utils.XLSXToCsv(fileXLSX, fileCSV);
            extractHashCodeSpan.addEvent("Conversion completed successfully");
        } catch (Exception e1) {
            extractHashCodeSpan.addEvent("Error during XLSX to CSV conversion");
            extractHashCodeSpan.setAttribute("conversion_error", true);
            //log.error("Error al transformar el fichero Xlsx a Csv", e1);
            extractHashCodeSpan.recordException(e1);
        }

        extractHashCodeSpan.addEvent("Reading CSV file");
        List<String> hashLines = FileUtils.readLines(fileCSV, "UTF-8");
        extractHashCodeSpan.setAttribute("number_of_lines_read", hashLines.size());

        for (String line : hashLines) {
            String[] valores = line.split(",");
            this.hashCodeOld.put(valores[0], valores[1]);
            extractHashCodeSpan.addEvent("Hash entry added", Attributes.of(
                AttributeKey.stringKey("key"), valores[0],
                AttributeKey.stringKey("hash_value"), valores[1]
            ));
        }

        extractHashCodeSpan.addEvent("Hashcode extraction completed successfully");
    } catch (IOException e) {
        extractHashCodeSpan.recordException(e);
        extractHashCodeSpan.setAttribute("error", true);
        extractHashCodeSpan.setAttribute("error.message", e.getMessage());
        //log.error("Error leyendo fichero hashcode", e);
    } finally {
        extractHashCodeSpan.end();
    }
}


	private void backup() {
		//log.debug("Init backup");
		//log.info("Comienza a hacerse el backup");
		File outputDirectoryFile = new File(outputFilesDirectoryString);
		File hashCSVFile = new File(Prop.fileHashCSV);
		File errorBigFile = new File(Prop.fileErrorBig);
		File errorNotFoundFile = new File(Prop.fileErrorNotFound);
		File errorGenericFile = new File(Prop.fileErrorGeneric);
		SimpleDateFormat formatFullDate = new SimpleDateFormat("yyyyMMdd");
		File copyDirectoryFile = null;
		File copyHashCSVFile = null;
		File copyErrorBigFile = null;
		File copyErrorNotFoundFile = null;
		File copyErrorGenericFile = null;
		if (outputDirectoryFile.exists()) {

			String copy = outputFilesDirectoryString + "_" + formatFullDate.format(new Date());
			copyDirectoryFile = new File(copy);
			int aux = 1;
			while (copyDirectoryFile.exists()) {
				copyDirectoryFile = new File(copy + "_" + aux++);
			}
		}
		if (hashCSVFile.exists()) {
			String s2 = Prop.fileHashCSV + "_" + formatFullDate.format(new Date());
			copyHashCSVFile = new File(s2);
			int aux = 1;
			while (copyHashCSVFile.exists()) {
				copyHashCSVFile = new File(s2 + "_" + aux++);
			}
		}
		if (errorBigFile.exists()) {
			String s3 = Prop.fileErrorBig + "_" + formatFullDate.format(new Date());
			copyErrorBigFile = new File(s3);
			int aux = 1;
			while (copyErrorBigFile.exists()) {
				copyErrorBigFile = new File(s3 + "_" + aux++);
			}
		}
		if (errorNotFoundFile.exists()) {
			String s4 = Prop.fileErrorNotFound + "_" + formatFullDate.format(new Date());
			copyErrorNotFoundFile = new File(s4);
			int aux = 1;
			while (copyErrorNotFoundFile.exists()) {
				copyErrorNotFoundFile = new File(s4 + "_" + aux++);
			}
		}
		if (errorGenericFile.exists()) {
			String s5 = Prop.fileErrorGeneric + "_" + formatFullDate.format(new Date());
			copyErrorGenericFile = new File(s5);
			int aux = 1;
			while (copyErrorGenericFile.exists()) {
				copyErrorGenericFile = new File(s5 + "_" + aux++);
			}
		}
		try {
			if (copyDirectoryFile != null)
				FileUtils.moveDirectoryToDirectory(outputDirectoryFile, copyDirectoryFile, true);
			if (copyHashCSVFile != null)
				FileUtils.copyFile(hashCSVFile, copyHashCSVFile);
			if (copyErrorBigFile != null)
				FileUtils.copyFile(errorBigFile, copyErrorBigFile);
			if (copyErrorNotFoundFile != null)
				FileUtils.copyFile(errorNotFoundFile, copyErrorNotFoundFile);
			if (copyErrorGenericFile != null)
				FileUtils.copyFile(errorGenericFile, copyErrorGenericFile);

		} catch (IOException e) {
			log.error("Error haciendo backup", e);
		}

		//log.info("Finaliza de hacerse el backup");
		//log.debug("End backup");
	}

	public List<String> getChanges() {
		return changes;
	}

	public void setChanges(List<String> changes) {
		this.changes = changes;
	}

	public List<String> getNews() {
		return news;
	}

	public void setNews(List<String> news) {
		this.news = news;
	}

	public HashMap<String, String> getIdDescription() {
		return idDescription;
	}

	public void setIdDescription(HashMap<String, String> idDescription) {
		this.idDescription = idDescription;
	}

	public static void main(String[] args) {
		if ((log == null) || (log.getLevel() == null))
			PropertyConfigurator.configure("log4j.properties");
			//log.info("Start process");
			Prop.loadConf();
			GenerateCSV app = new GenerateCSV(args[1], args[2]);
//			app.backup();
			if (args[0].equals("update")) {
				app.extractFiles();
			}else if(args[0].equals("generateHash")){
				app.generateHashCodeFromBI();
			}
			//log.info("Finish process");
		}

}
