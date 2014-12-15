package de.mannheim.uni.lod;

import java.util.ArrayList;
import java.util.List;

import com.hp.hpl.jena.query.ParameterizedSparqlString;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;

public class SPARQLEndpointQueryRunner {
	public final static String DBPEDIA_ENDPOINT = "http://wifo5-32.informatik.uni-mannheim.de:8891/sparql";
	public final static String LOCAL_DBPEDIA_ENDPOINT = "http://wifo5-38.informatik.uni-mannheim.de:8890/sparql";
	public final static String YAGO_ENDPOINT = "http://wifo5-38.informatik.uni-mannheim.de:8890/sparql";// "http://lod2.openlinksw.com/sparql";

	public static final String GET_SUPERCLASSES_QUERY = "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> select ?superclass where { ?class rdfs:subClassOf* ?superclass}";

	public static final String GET_DIRECT_SUPERCLASSES_QUERY = "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> select ?superclass where { ?class rdfs:subClassOf ?superclass}";

	public static final String GET_SUBCLASSES_QUERY = "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> select ?subclass where { ?subclass rdfs:subClassOf* ?class  } OPTION (transitive, t_distinct, t_max(1))";

	public static final String GET_CLASS_FROM_LABEL = "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> select ?s  where {?s <http://www.w3.org/2000/01/rdf-schema#label> \"$CLASS\"@eng .  FILTER EXISTS {?o a ?s}  }";

	private String endpoint;

	private String alias;

	private int timeout;

	private int retries;

	private int pageSize;

	private boolean useCount;

	private boolean usePropertyPaths;

	private QueryExecution objectToExec;

	public boolean isUseCount() {
		return useCount;
	}

	public void setUseCount(boolean useCount) {
		this.useCount = useCount;
	}

	public boolean isUsePropertyPaths() {
		return usePropertyPaths;
	}

	public void setUsePropertyPaths(boolean usePropertyPaths) {
		this.usePropertyPaths = usePropertyPaths;
	}

	public String getEndpoint() {
		return endpoint;
	}

	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}

	public String getAlias() {
		return alias;
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}

	public int getTimeout() {
		return timeout;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public int getRetries() {
		return retries;
	}

	public void setRetries(int retries) {
		this.retries = retries;
	}

	public int getPageSize() {
		return pageSize;
	}

	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}

	public SPARQLEndpointQueryRunner(String endpoint, String alias,
			int timeout, int retries, int pageSize, boolean useCount,
			boolean usePropertyPaths) {
		super();
		this.endpoint = endpoint;
		this.alias = alias;
		this.timeout = timeout;
		this.retries = retries;
		this.pageSize = pageSize;
		this.useCount = useCount;
		this.usePropertyPaths = usePropertyPaths;
	}

	public SPARQLEndpointQueryRunner(String endpoint, int timeout, int retries) {
		super();
		this.endpoint = endpoint;
		this.timeout = timeout;
		this.retries = retries;
	}

	public SPARQLEndpointQueryRunner(String endpoint) {
		super();
		this.endpoint = endpoint;
		this.timeout = 60 * 1000;
		this.retries = 10;
		this.pageSize = 10000;
	}

	public static SPARQLEndpointQueryRunner getDBpeidaRunner() {
		SPARQLEndpointQueryRunner runner = new SPARQLEndpointQueryRunner(
				DBPEDIA_ENDPOINT);
		runner.setPageSize(10000);
		return runner;
	}

	public static SPARQLEndpointQueryRunner getLocalDBpeidaRunner() {
		SPARQLEndpointQueryRunner runner = new SPARQLEndpointQueryRunner(
				LOCAL_DBPEDIA_ENDPOINT);
		runner.setPageSize(10000);
		return runner;
	}

	public ResultSet runSelectQuery(String query) {
		Query q = QueryFactory.create(query);
		objectToExec = QueryExecutionFactory.sparqlService(endpoint,
				q.toString());
		objectToExec.setTimeout(timeout);
		// retry every 1000 millis if the endpoint goes down
		int localRetries = 0;
		ResultSet results = null;
		while (true) {
			try {
				results = objectToExec.execSelect();
				break;
			} catch (Exception ex) {
				ex.printStackTrace();
				localRetries++;
				// if (localRetries >= retries) {
				// ex.printStackTrace();
				// break;
				// }
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		return results;
	}

	public void closeConnection() {
		objectToExec.close();
	}

	public static Query addOrderByToQuery(String queryStr) {
		Query querQ = QueryFactory.create(queryStr);
		for (String str : querQ.getResultVars()) {
			querQ.addOrderBy(str, 0);
		}
		// remove the prefixes from the subquery
		String prefixes = "";
		String noPrefixQuery = querQ.toString();
		if (querQ.toString().toLowerCase().contains("select")) {
			prefixes = querQ.toString().substring(0,
					querQ.toString().toLowerCase().indexOf("select"));
			if (prefixes.toLowerCase().contains("prefix")) {
				noPrefixQuery = querQ.toString().replace(prefixes, "");

			}
		}
		// add the subquery
		String outsideQuery = "SELECT";
		for (String str : querQ.getResultVars()) {
			outsideQuery += " ?" + str;
		}
		String finalQuery = outsideQuery + " WHERE { {" + noPrefixQuery + "} }";

		querQ = QueryFactory.create(prefixes + finalQuery);
		return querQ;
	}

	public List<String> getSuperClasses(String classUri) {
		List<String> superClasses = new ArrayList<String>();
		try {
			ParameterizedSparqlString queryString = new ParameterizedSparqlString(
					GET_SUPERCLASSES_QUERY);
			queryString.setIri("?class", classUri);
			ResultSet results = runSelectQuery(queryString.toString());
			while (results.hasNext()) {
				QuerySolution solution = results.next();
				String superClass = solution.get("superclass").toString();
				if (superClass.startsWith("http"))
					superClasses.add(superClass);
			}
			if (superClasses.contains(classUri))
				superClasses.remove(classUri);
		} catch (Exception e) {

		}
		closeConnection();
		return superClasses;
	}

	public String getClassFromLabel(String label) {
		try {
			String qStr = GET_CLASS_FROM_LABEL.replace("$CLASS",
					label.toLowerCase());
			ResultSet results = runSelectQuery(qStr);
			while (results.hasNext()) {
				QuerySolution solution = results.next();
				String superClass = solution.get("s").toString();
				return superClass;
			}
		} catch (Exception e) {

		}
		closeConnection();
		return null;

	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SPARQLEndpointQueryRunner qr = new SPARQLEndpointQueryRunner(
				"http://dbpedia.org/sparql");
		// qr.getSubClasses("http://dbpedia.org/class/yago/Object100002684",
		// new ArrayList<String>());
	}

}
