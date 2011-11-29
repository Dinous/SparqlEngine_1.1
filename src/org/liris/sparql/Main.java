/**.
 * 
 */
package org.liris.sparql;

import java.io.ByteArrayInputStream;
import java.io.PrintStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.liris.ktbs.client.Ktbs;
import org.liris.ktbs.client.KtbsClient;
import org.liris.ktbs.client.KtbsConstants;
import org.liris.ktbs.dao.ResultSet;
import org.liris.ktbs.domain.interfaces.IComputedTrace;
import org.liris.ktbs.domain.interfaces.IMethod;
import org.liris.ktbs.domain.interfaces.IMethodParameter;
import org.liris.ktbs.domain.interfaces.IObsel;
import org.liris.ktbs.domain.interfaces.ITrace;
import org.liris.ktbs.serial.DeserializationConfig;
import org.liris.ktbs.serial.LinkAxis;
import org.liris.ktbs.serial.SerializationConfig;
import org.liris.ktbs.serial.SerializationMode;
import org.liris.ktbs.serial.Serializer;
import org.liris.ktbs.serial.SerializerFactory;
import org.liris.ktbs.serial.rdf.RdfDeserializer;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.Syntax;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

/**.
 * @author Dino
 * @21 juin 2011
 */
public class Main {

    private static final String kTBS_URL = "http://localhost:8001/";
    private KtbsClient client = Ktbs.getRestClient(kTBS_URL);
    private SerializerFactory factory = new SerializerFactory();
    //private static  Logger log = LoggerFactory.getLogger(Main.class); 
    
    /**.
     * @param args => sourceTraceUri, targetTraceUri
     */
    public static void main(String[] args) {
	System.out.println("<!--");
	
//	try {
//	    InputStream logback = ClassLoader.getSystemResourceAsStream("logback.xml");
//	    InputStreamReader streamReader = new InputStreamReader(logback);
//	    //le buffer permet le readline
//	    BufferedReader buffer=new BufferedReader(streamReader);
//	    StringWriter writer=new StringWriter();
//	    String line="";
//	    while ( null!=(line=buffer.readLine())){
//	    writer.write(line); 
//	    }
//	    // Sortie finale dans le String
//	    System.out.println(writer.toString());
//	} catch (IOException e) {
//	    // TODO Auto-generated catch block
//	    e.printStackTrace();
//	}
	//log.warn("début du main");
	try{
	if(args.length == 2){
	    String sourceTraceUri = args[0];
	    String targetTraceUri = args[1];
	    
	    Main main = new Main();
	    if (main.check(/*methodUri, */sourceTraceUri, targetTraceUri/*, targetTraceParameterName, baseUri*/))
	    main.execute(/*methodUri,*/ sourceTraceUri, targetTraceUri/*, targetTraceParameterName, baseUri*/);
	    
	}else{
	    System.err.println("sparql1.1_4j required 2 (String) parameters in this order : sourceTraceUri, targetTraceUri");
	}
	}catch(Exception ex){
	    System.err.println("Error Logging");
	}
	//log.warn("fin du main");
    }
    
    private boolean checkTrace(String traceUri){
	boolean ret = true;
	ITrace sourceTrace = client.getResourceService().getTrace(traceUri);
	if(sourceTrace == null){
	    //log.warn("SourceTrace '"+traceUri+"' missing in kTBS '"+ kTBS_URL + "' ");
	    ret =  false;
	}else{
	    //log.warn("SourceTrace '"+traceUri+"' : checked ! ");
	}
	return ret;
    }
    
    private boolean check(/*String methodUri, -*/String sourceTraceUri, String targetTraceUri/*, String targetTraceParamterName, String baseUri*/){
	boolean ret = true;
	
	if(!checkTrace(sourceTraceUri)){
	    return false;
	}
	
	if(!checkTrace(targetTraceUri)){
	    return false;
	}
	
//	IBase base =  client.getResourceService().getBase(baseUri);
//	if(base == null){
//	    //log.warn("Base '"+baseUri+"' missing in kTBS '"+ kTBS_URL + "' ");
//	    ret =  false;
//	}else{
//	    //log.warn("Base '"+baseUri+"' : checked ! ");
//	}
	
//	ITrace trace = client.getResourceService().getTrace(sourceTraceUri);
//	if(trace == null){
//	    //log.warn("Trace '"+sourceTraceUri+"' missing in kTBS '"+ kTBS_URL + "' ");
//	    ret =  false;
//	}else{
//	    if(!(trace.getParentResource() instanceof IBase)){
//		//log.warn("Trace '"+sourceTraceUri+"' doesn't a child of the base '"+ baseUri + "' ");
//		ret =  false;
//	    }else{
//		    //log.warn("Trace '"+sourceTraceUri+"' : checked ! ");
//		}
//	}
	return ret;
    }
    
    /**
     * .
     * @param methodUri			Uri de la méthode à executer
     * @param sourceTraceUri		Uri de la trace source
     * @param targetTraceUri		Uri de la trace de destination
     * @param targetTraceParamterName	Nom du paramètre (dans la méthode) qui prendra comme valeur l URI de la trace cible
     * @param baseUri			Uri de la base de trace
     */
    private void execute(/*String methodUri, */String sourceTraceUri, String targetTraceUri/*, String targetTraceParamterName, String baseUri*/){
	
	System.out.println("-->");
	//log.warn("Récupération de la requete SPARQL à exécuter");
	String methodUri = null;
	ITrace sourceTrace = client.getResourceService().getTrace(targetTraceUri);
	IComputedTrace computeTrace = ((IComputedTrace)sourceTrace);
	if(computeTrace.getMethod() != null){
	    methodUri = ((IComputedTrace)sourceTrace).getMethod().getUri();
	}
	
	List<String> methodStrs = strMethod(methodUri, targetTraceUri, "__destination__");
	//log.warn("Récupération des obsels à traiter");
	String strListObsel = strListObsel(sourceTraceUri);

	//log.warn("Pour chaque méthode, exécution sur les obsels");
	for (String methodStr : methodStrs) {
	    //log.warn("Execution de "+methodStr);    
	    String executeQuery;
	    try {
		executeQuery = executeQuery(methodStr, strListObsel);
		if(StringUtils.isEmpty(executeQuery)){
		    executeQuery = "<rdf:RDF />";
		}
		//log.warn(executeQuery);
		PrintStream out = new PrintStream(System.out, true, "UTF-8");
		out.println(executeQuery);
		//System.out.println(executeQuery);
		
	    } catch (Exception e) {
		//log.severe("Mauvais encodage");
	    }
	    
	    //listObsel(executeQuery, baseUri);    
	}
	
    }
    
    /**.
     * @param methodStr
     * @param listObsel
     * @return
     * @throws UnsupportedEncodingException 
     */
    private String executeQuery(String methodStr, String listObsel) throws UnsupportedEncodingException {
	
	Model trace = ModelFactory.createDefaultModel();
	trace.read(new ByteArrayInputStream(listObsel.getBytes("UTF-8")) , "", KtbsConstants.JENA_TURTLE);
	//log.warn(trace.toString());
	
	Query query = QueryFactory.create(methodStr, Syntax.syntaxARQ);

	//log.warn(query.toString());
	
	// Execute the query and obtain results
	QueryExecution qe = QueryExecutionFactory.create(query, trace);
	Model resultModel = qe.execConstruct();
	 StringWriter writer = new StringWriter();
	resultModel.write(writer, KtbsConstants.JENA_RDF_XML, null);
	return writer.toString();
    }

    private List<String> strMethod(String methodUri, String targetTraceUri, String targetTraceParamterName){
	List<String> ret = new LinkedList<String>();
	IMethod method = client.getResourceService().getMethod(methodUri);
	
	if(method.getInherits().equals(KtbsConstants.SUPER_METHOD)  ){
	    for(IMethodParameter methodParameter : method.getMethodParameters()){
	    	    if(methodParameter.getName().equals("submethods")){
	    		String[] stringMethodLocalNames = methodParameter.getValue().split(" ");
	    		for (String stringMethodLocalName : stringMethodLocalNames) {
	    		IMethod meth = client.getResourceService().getMethod(stringMethodLocalName);
	    		 ret.add(sparqlRequest(meth,targetTraceParamterName,targetTraceUri));
			}
	    		break;
	    	    }
	    	}
	}else{
	    ret.add(sparqlRequest(method, targetTraceParamterName,targetTraceUri));
	}
	
	return ret;

    }
    
    private String sparqlRequest(IMethod method, String targetTraceParamterName, String targetTraceUri){
	String ret = "";
	for(IMethodParameter methodParameter : method.getMethodParameters()){
    	    if(methodParameter.getName().equals("sparql")){
    		ret = methodParameter.getValue();
    		break;
    	    }
    	}
    	if(ret != null){
    	    ret = StringUtils.replace(ret, "%("+targetTraceParamterName+")s", targetTraceUri);
    	}
	return ret;
    }
    
    private String strListObsel(String traceUri){
	 ITrace trace = client.getResourceService().getTrace(traceUri);
         
         SerializationConfig config = new SerializationConfig();
         config.configure(LinkAxis.CHILD, SerializationMode.CASCADE);
         config.configure(LinkAxis.LINKED, SerializationMode.URI);
         config.configure(LinkAxis.LINKED_SAME_TYPE, SerializationMode.URI);
         config.configure(LinkAxis.PARENT, SerializationMode.URI);
         
         Serializer serializer = factory.newRdfSerializer(config);
         
         StringWriter writer = new StringWriter();
         serializer.serializeResource(writer, trace, KtbsConstants.MIME_TURTLE);
         
         return writer.toString();
    }

    @SuppressWarnings("unused")
    private Set<IObsel> listObsel(String strListObsel, String baseUri){
        DeserializationConfig config = new DeserializationConfig();
        RdfDeserializer deserializer = factory.newRdfDeserializer(client.getProxyFactory(), config);
        
        StringReader reader = new StringReader(strListObsel);
        
        ResultSet<IObsel> listObsel = deserializer.deserializeResourceSet(IObsel.class, reader, baseUri, KtbsConstants.MIME_RDF_XML);
        
        return (Set<IObsel>) listObsel;
   }
}
