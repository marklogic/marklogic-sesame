package com.marklogic.sesame.functionaltests;

import static org.hamcrest.core.AnyOf.anyOf;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.Iteration;
import info.aduna.iteration.Iterations;
import info.aduna.iteration.IteratorIteration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openrdf.IsolationLevel;
import org.openrdf.IsolationLevels;
import org.openrdf.OpenRDFException;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.Query;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.UnsupportedQueryLanguageException;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.query.Update;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.repository.config.RepositoryConfigException;
import org.openrdf.repository.config.RepositoryFactory;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RioSetting;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.Authentication;
import com.marklogic.client.FailedRequestException;
import com.marklogic.client.document.XMLDocumentManager;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.QueryDefinition;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.RawCombinedQueryDefinition;
import com.marklogic.client.query.StringQueryDefinition;
import com.marklogic.client.query.StructuredQueryBuilder;
import com.marklogic.client.semantics.Capability;
import com.marklogic.client.semantics.GraphManager;
import com.marklogic.client.semantics.SPARQLRuleset;
import com.marklogic.semantics.sesame.MarkLogicRepository;
import com.marklogic.semantics.sesame.MarkLogicRepositoryConnection;
import com.marklogic.semantics.sesame.MarkLogicTransactionException;
import com.marklogic.semantics.sesame.config.MarkLogicRepositoryConfig;
import com.marklogic.semantics.sesame.config.MarkLogicRepositoryFactory;
import com.marklogic.semantics.sesame.query.MarkLogicBooleanQuery;
import com.marklogic.semantics.sesame.query.MarkLogicQuery;
import com.marklogic.semantics.sesame.query.MarkLogicTupleQuery;
import com.marklogic.semantics.sesame.query.MarkLogicUpdateQuery;
import com.marklogic.sesame.functionaltests.util.ConnectedRESTQA;
import com.marklogic.sesame.functionaltests.util.StatementIterable;
import com.marklogic.sesame.functionaltests.util.StatementIterator;
import com.marklogic.sesame.functionaltests.util.StatementList;

public class MarkLogicRepositoryConnectionTest  extends  ConnectedRESTQA{

	private static final String TEST_DIR_PREFIX = "/testdata/";
	private static String dbName = "MLSesame";
	private static String [] fNames = {"MLSesame-1"};
	private static String restServer = "REST-MLSesame-API-Server";
	private static int restPort = 8011;
		
	protected static DatabaseClient databaseClient ;
	protected static MarkLogicRepository testAdminRepository;
	protected static MarkLogicRepository testReaderRepository;
	protected static MarkLogicRepository testWriterRepository;
	protected static MarkLogicRepositoryConnection testAdminCon;
	protected static MarkLogicRepositoryConnection testReaderCon;
	protected static MarkLogicRepositoryConnection testWriterCon;
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());
	protected ValueFactory vf;
	protected ValueFactory vfWrite;
	protected URI graph1;
	protected URI graph2;
	protected URI dirgraph;
	protected URI dirgraph1;
	
	protected URI john;
	protected URI micah;
	protected URI fei;
	
	protected URI fname;
	protected URI lname;
	protected URI email;
	protected URI homeTel;
	
	protected Literal johnfname;
	protected Literal johnlname;
	protected Literal johnemail;
	protected Literal johnhomeTel;
	protected Literal micahfname;
	protected Literal micahlname;
	protected Literal micahhomeTel;
	protected Literal feifname;
	protected Literal feilname;
	protected Literal feiemail;
	
	protected URI writeFuncSpecOf ;
	protected URI type        ;
	protected URI worksFor    ;
	protected URI developPrototypeOf ;
	protected URI ml          ;
	protected URI semantics   ;
	protected URI inference   ;
	protected URI sEngineer   ;
	protected URI lEngineer   ;
	protected URI engineer    ;
	protected URI employee    ;
	protected URI design      ;
	protected URI subClass    ;
	protected URI subProperty ;
	protected URI eqProperty  ;
	protected URI develop  ;
	protected QueryManager qmgr;
	
	private static final String ID = "id";
	private static final String ADDRESS = "addressbook";
	protected static final String NS = "http://marklogicsparql.com/";
	protected static final String RDFS = "http://www.w3.org/2000/01/rdf-schema#>";
	protected static final String OWL = "http://www.w3.org/2002/07/owl#";
	
	@BeforeClass
	public static void initialSetup() throws Exception {/*
		
		
		setupJavaRESTServer(dbName, fNames[0], restServer, restPort);
		setupAppServicesConstraint(dbName);
		enableCollectionLexicon(dbName);
		enableTripleIndex(dbName);
		
		//createUserRolesWithPrevilages("test-eval", "xdbc:eval", "xdbc:eval-in", "xdmp:eval-in", "any-uri", "xdbc:invoke");
		createRESTUser("reader", "reader", "rest-reader");
		createRESTUser("writer", "writer", "rest-writer");
		
	*/
	}
	
/*	@AfterClass
	public static void tearDownSetup() throws Exception  {
		tearDownJavaRESTServer(dbName, fNames, restServer);
		deleteUserRole("test-eval");
		deleteRESTUser("reader");
		deleteRESTUser("writer");
	}*/
	
	@Before
	public void setUp()
		throws Exception
	{	
		logger.debug("Initializing repository");
		createRepository();
		
		vf = testAdminCon.getValueFactory();
		vfWrite = testWriterCon.getValueFactory();
		
		john = vf.createURI(NS+ID+"#1111");
		micah = vf.createURI(NS+ID+"#2222");
		fei = vf.createURI(NS+ID+"#3333");
		
		
		fname = vf.createURI(NS+ADDRESS+"#firstName");
		lname = vf.createURI(NS+ADDRESS+"#lastName");
		email = vf.createURI(NS+ADDRESS+"#email");
		homeTel =vf.createURI(NS+ADDRESS+"#homeTel");
		
		writeFuncSpecOf =vf.createURI(NS+"writeFuncSpecOf");
		type = vf.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
		worksFor =vf.createURI(NS+"worksFor");
		
		developPrototypeOf =vf.createURI(NS+"developPrototypeOf");
		ml =vf.createURI(NS+"MarkLogic");
		
		
		semantics = vf.createURI(NS+"Semantics");
		inference = vf.createURI(NS+"Inference");
		sEngineer = vf.createURI(NS+"SeniorEngineer");
		lEngineer = vf.createURI(NS+"LeadEngineer");
		engineer = vf.createURI(NS+"Engineer");
		employee = vf.createURI(NS+"Employee");
		design = vf.createURI(NS+"design");
		develop = vf.createURI(NS+"develop");
		
		subClass = vf.createURI(RDFS+"design");
		subProperty = vf.createURI(RDFS+"design");
		eqProperty = vf.createURI(OWL+"equivalentProperty");
				
		johnfname = vf.createLiteral("John");
		johnlname = vf.createLiteral("Snelson");
		johnhomeTel = vf.createLiteral(111111111D);
		johnemail = vf.createLiteral("john.snelson@marklogic.com");
		
		micahfname = vf.createLiteral("Micah");
		micahlname = vf.createLiteral("Dubinko");
		micahhomeTel = vf.createLiteral(22222222D);
		
		feifname = vf.createLiteral("Fei");
		feilname = vf.createLiteral("Ling");
		feiemail = vf.createLiteral("fei.ling@marklogic.com");
	}

	@After
	public void tearDown()
		throws Exception
	{
		testAdminCon.clear();
		testAdminCon.close();
		testAdminRepository.shutDown();
		testAdminRepository = null;
		testAdminCon = null;

		
		testReaderCon.close();
		testReaderRepository.shutDown();
		testReaderRepository = null;
		testReaderCon = null;
		
		testWriterCon.close();
		testWriterRepository.shutDown();
		testWriterCon = null; 
		testWriterRepository = null;
        logger.info("tearDown complete.");
        Thread.currentThread().sleep(1000);
	}

	/**
	 * Gets an (uninitialized) instance of the repository that should be tested.
	 * 
	 * @return void
	 * @throws RepositoryConfigException 
	 * @throws RepositoryException 
	 */
	protected void  createRepository() throws Exception {
		
		//Creating MLSesame Connection object Using MarkLogicRepositoryConfig
		
		MarkLogicRepositoryConfig adminconfig = new MarkLogicRepositoryConfig();
		adminconfig.setHost("localhost");
		adminconfig.setAuth("DIGEST");
		adminconfig.setUser("admin");
		adminconfig.setPassword("admin");
		adminconfig.setPort(restPort);
		RepositoryFactory factory = new MarkLogicRepositoryFactory();
        Assert.assertEquals("marklogic:MarkLogicRepository", factory.getRepositoryType());
        try {
			testAdminRepository = (MarkLogicRepository) factory.getRepository(adminconfig);
		} catch (RepositoryConfigException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        try {
			testAdminRepository.initialize();
			testAdminCon = (MarkLogicRepositoryConnection) testAdminRepository.getConnection();
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        // Creating testAdminCon with MarkLogicRepositoryConfig constructor
        testAdminCon.close();
        testAdminRepository.shutDown();
        testAdminRepository = null; 
        testAdminCon = null; 
        
        adminconfig = new MarkLogicRepositoryConfig("localhost",restPort,"admin","admin","DIGEST");
        Assert.assertEquals("marklogic:MarkLogicRepository", factory.getRepositoryType());
        testAdminRepository = (MarkLogicRepository) factory.getRepository(adminconfig);
        testAdminRepository.initialize();
        testAdminCon = testAdminRepository.getConnection();
        Assert.assertTrue(testAdminCon instanceof MarkLogicRepositoryConnection);
        
        Repository otherrepo =  factory.getRepository(adminconfig);
        try{
        	 //try to get connection without initialising repo, will throw error
            RepositoryConnection conn = otherrepo.getConnection();
            Assert.assertTrue(false);
       }
        catch(Exception e){
        	Assert.assertTrue(e instanceof RepositoryException);
        	otherrepo.shutDown();
        }
        
        Assert.assertTrue(testAdminCon instanceof MarkLogicRepositoryConnection);
        graph1 = testAdminCon.getValueFactory().createURI("http://marklogic.com/Graph1");
        graph2 = testAdminCon.getValueFactory().createURI("http://marklogic.com/Graph2");
        dirgraph = testAdminCon.getValueFactory().createURI("http://marklogic.com/dirgraph");
        dirgraph1 = testAdminCon.getValueFactory().createURI("http://marklogic.com/dirgraph1");
        
        
       //Creating MLSesame Connection object Using MarkLogicRepository overloaded constructor
        
        testReaderRepository = new MarkLogicRepository("localhost", restPort, "reader", "reader", "DIGEST");
        try {
			testReaderRepository.initialize();
			Assert.assertNotNull(testReaderRepository);
			testReaderCon = (MarkLogicRepositoryConnection) testReaderRepository.getConnection();
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        Assert.assertTrue(testReaderCon instanceof MarkLogicRepositoryConnection);
       
        //Creating MLSesame Connection object Using MarkLogicRepository(databaseclient)  constructor
        
		databaseClient = DatabaseClientFactory.newClient("localhost", restPort, "writer", "writer", DatabaseClientFactory.Authentication.valueOf("DIGEST"));
		testWriterRepository = new MarkLogicRepository(databaseClient);
		qmgr = databaseClient.newQueryManager();
		
		try {
			testWriterRepository.initialize();
			Assert.assertNotNull(testWriterRepository);
			testWriterCon = (MarkLogicRepositoryConnection) testWriterRepository.getConnection();
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	//ISSUE - 19
	@Test
	public void testPrepareBooleanQuery1() throws Exception{
		
		Assert.assertEquals(0L, testAdminCon.size());
		InputStream in = MarkLogicRepositoryConnectionTest.class.getResourceAsStream(TEST_DIR_PREFIX
				+ "tigers.ttl");
		testAdminCon.add(in, "", RDFFormat.TURTLE);
		in.close();
		Assert.assertEquals(107L, testAdminCon.size());
		
		String query1 = "PREFIX  bb: <http://marklogic.com/baseball/players#>"+
						" ASK "+
						" WHERE"+ 
						" {"+
						" ?id bb:lastname  ?name ."+
						" FILTER  EXISTS { ?id bb:country ?countryname }"+
						" }";
		
		boolean result1 = testAdminCon.prepareBooleanQuery(QueryLanguage.SPARQL, query1).evaluate();
		Assert.assertFalse(result1);	
		
		String query2 = "PREFIX  bb: <http://marklogic.com/baseball/players#>"+
				"PREFIX  r: <http://marklogic.com/baseball/rules#>"+
				" ASK WHERE"+ 
				" {"+
				 " ?id bb:team r:Tigers."+
				    " ?id bb:position \"pitcher\"."+
				" }";

		boolean result2 = testAdminCon.prepareBooleanQuery(QueryLanguage.SPARQL, query2).evaluate();
		Assert.assertTrue(result2);	
	}
	
	@Test
	public void testPrepareBooleanQuery2() throws Exception{
	
			
		InputStream in = MarkLogicRepositoryConnectionTest.class.getResourceAsStream(TEST_DIR_PREFIX
				+ "tigers.ttl");		
		Reader reader = new InputStreamReader(in);
		testAdminCon.add(reader, "http://marklogic.com/baseball/", RDFFormat.TURTLE, graph1);
		reader.close();
		
		Assert.assertEquals(107L, testAdminCon.size());
		
		
		String query1 = "ASK FROM <http://marklogic.com/Graph1>"+
				" WHERE"+ 
				" {"+
				 " ?player ?team <#Tigers>."+
				 " }";

		boolean result1 = testAdminCon.prepareBooleanQuery(QueryLanguage.SPARQL, query1,"http://marklogic.com/baseball/rules").evaluate();
		Assert.assertTrue(result1);	
		
		String query2 = "PREFIX  bb: <http://marklogic.com/baseball/players#>"+
				" PREFIX  r: <http://marklogic.com/baseball/rules#>"+
				" ASK  FROM <http://marklogic.com/Graph1> WHERE"+ 
				" {"+
				 " ?id bb:team r:Tigers."+
				    " ?id bb:position \"pitcher\"."+
				" }";

		boolean result2 = testAdminCon.prepareBooleanQuery(QueryLanguage.SPARQL, query2, "").evaluate();
		Assert.assertTrue(result2);	
		
	}
	
	@Test
	public void testPrepareBooleanQuery3() throws Exception{
		
		URL url = MarkLogicRepositoryConnectionTest.class.getResource(TEST_DIR_PREFIX+"tigers.ttl");
				
		testAdminCon.add(url, "", RDFFormat.TURTLE, graph1);
		
		Assert.assertEquals(107L, testAdminCon.size());
			
		String query1 = "PREFIX  bb: <http://marklogic.com/baseball/players#>"+
				"ASK "+
				"WHERE"+ 
				"{"+
				" ?s bb:position ?o."+
				 "}";

		BooleanQuery bq = testAdminCon.prepareBooleanQuery(query1);
		bq.setBinding("o", vf.createLiteral("coach"));
		boolean result1	= bq.evaluate();
		Assert.assertTrue(result1);	
		
		bq.setBinding("o", vf.createLiteral("pitcher"));
		boolean result2	= bq.evaluate();
		Assert.assertTrue(result2);
		
		bq.setBinding("o", vf.createLiteral("abcd"));
		boolean result3	= bq.evaluate();
		Assert.assertFalse(result3);
	}
	
	@Test
	public void testPrepareBooleanQuery4() throws Exception{
		
		File file = new File(MarkLogicRepositoryConnectionTest.class.getResource(TEST_DIR_PREFIX+ "tigers.ttl").getFile());		
		testAdminCon.add(file, "", RDFFormat.TURTLE, graph1);
		logger.debug(file.getAbsolutePath());
		
		
		Assert.assertEquals(107L, testAdminCon.size(graph1));
		
		
		String query1 = "PREFIX  bb: <http://marklogic.com/baseball/players#>"+
				"ASK FROM <http://marklogic.com/Graph1>"+
				"WHERE"+ 
				"{"+
				 "<#119> <#lastname> \"Verlander\"."+
				 "<#119> <#team> ?tigers."+
				 "}";

		boolean result1 = testAdminCon.prepareBooleanQuery(query1,"http://marklogic.com/baseball/players").evaluate();
		Assert.assertTrue(result1);	
	}
	
	// ISSUE 20 , 25
	@Test
	public void testPrepareTupleQuery1() throws Exception{
		
		Assert.assertEquals(0, testAdminCon.size());
		
		Statement st1 = vf.createStatement(john, fname, johnfname, dirgraph);
		Statement st2 = vf.createStatement(john, lname, johnlname, dirgraph);
		Statement st3 = vf.createStatement(john, homeTel, johnhomeTel, dirgraph);
		Statement st4 = vf.createStatement(john, email, johnemail, dirgraph);
		Statement st5 = vf.createStatement(micah, fname, micahfname, dirgraph);
		Statement st6 = vf.createStatement(micah, lname, micahlname, dirgraph);
		Statement st7 = vf.createStatement(micah, homeTel, micahhomeTel, dirgraph);
		Statement st8 = vf.createStatement(fei, fname, feifname, dirgraph);
		Statement st9 = vf.createStatement(fei, lname, feilname, dirgraph);
		Statement st10 = vf.createStatement(fei, email, feiemail, dirgraph);
		
		testAdminCon.add(st1, dirgraph);
		testAdminCon.add(st2, dirgraph);
		testAdminCon.add(st3, dirgraph);
		testAdminCon.add(st4, dirgraph);
		testAdminCon.add(st5, dirgraph);
		testAdminCon.add(st6, dirgraph);
		testAdminCon.add(st7, dirgraph);
		testAdminCon.add(st8, dirgraph);
		testAdminCon.add(st9, dirgraph);
		testAdminCon.add(st10, dirgraph);
		
					
		Assert.assertEquals(10, testAdminCon.size(dirgraph));	
		
		StringBuilder queryBuilder = new StringBuilder();
		queryBuilder.append("PREFIX ad: <http://marklogicsparql.com/addressbook#>");
		queryBuilder.append(" PREFIX d:  <http://marklogicsparql.com/id#>");
		queryBuilder.append("         SELECT DISTINCT ?person");
		queryBuilder.append(" FROM <http://marklogic.com/dirgraph>");
		queryBuilder.append(" WHERE");
		queryBuilder.append(" {?person ad:firstName ?firstname ;");
		queryBuilder.append(" ad:lastName ?lastname.");
		queryBuilder.append(" OPTIONAL {?person ad:homeTel ?phonenumber .}");
		queryBuilder.append(" FILTER (?firstname = \"Fei\")}");
		
		TupleQuery query = testAdminCon.prepareTupleQuery(QueryLanguage.SPARQL, queryBuilder.toString());
		TupleQueryResult result = query.evaluate();
		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(true)));
			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertThat(solution.hasBinding("person"), is(equalTo(true)));
				Value nameResult = solution.getValue("person");
				Assert.assertEquals(nameResult.stringValue(),fei.stringValue());
			}
		}
		finally {
			result.close();
		}
	
	}
	
	@Test
	public void testPrepareTupleQuery2() throws Exception{
		
		testAdminCon.add(john, fname, johnfname, dirgraph);
		testAdminCon.add(john, lname, johnlname, dirgraph);
		testAdminCon.add(john, homeTel, johnhomeTel, dirgraph);
		testAdminCon.add(john, email, johnemail, dirgraph);
		
		testAdminCon.add(micah, fname, micahfname, dirgraph);
		testAdminCon.add(micah, lname, micahlname, dirgraph);
		testAdminCon.add(micah, homeTel, micahhomeTel, dirgraph);
		

		testAdminCon.add(fei, fname, feifname, dirgraph);
		testAdminCon.add(fei, lname, feilname, dirgraph);
		testAdminCon.add(fei, email, feiemail, dirgraph);
		
		try{
			Assert.assertEquals(10, testAdminCon.size(dirgraph));
		}
		catch(Exception ex){
			logger.error("Failed :", ex);
		}
			
		
		StringBuilder queryBuilder = new StringBuilder();
		
		queryBuilder.append("PREFIX ad: <http://marklogicsparql.com/addressbook#>");
		queryBuilder.append(" PREFIX d:  <http://marklogicsparql.com/id#>");
		queryBuilder.append("         SELECT ?person ?lastname");
		queryBuilder.append(" WHERE");
		queryBuilder.append(" {?person <#firstName> ?firstname ;");
		queryBuilder.append(" <#lastName> ?lastname.");
		queryBuilder.append(" OPTIONAL {?person <#email> ?email.}");
		queryBuilder.append("  FILTER  EXISTS  {?person <#homeTel> ?tel .}} ORDER BY ?lastname");
		
		TupleQuery query = testAdminCon.prepareTupleQuery(QueryLanguage.SPARQL, queryBuilder.toString(),"http://marklogicsparql.com/addressbook");
		TupleQueryResult result = query.evaluate();
		
		String [] expectedPersonresult = {micah.stringValue(), john.stringValue()};
		String [] expectedLnameresult = {micahlname.stringValue(), johnlname.stringValue()};
		int i = 0;
		try {
			assertThat(result, is(notNullValue()));
			Assert.assertTrue(result.hasNext());
			while (result.hasNext()) {
				BindingSet solution = result.next();
				
				assertThat(solution.hasBinding("person"), is(equalTo(true)));
				assertThat(solution.hasBinding("lastname"), is(equalTo(true)));
				
				Value personResult = solution.getValue("person");
				Value nameResult = solution.getValue("lastname");
				
				Assert.assertEquals(personResult.stringValue(),expectedPersonresult[i]);
				Assert.assertEquals(nameResult.stringValue(),expectedLnameresult[i]);
				i++;
			}
		}
		finally {
			result.close();
		}
	
	}

	@Test
	public void testPrepareTupleQuery3() throws Exception{
		
		Statement st1 = vf.createStatement(john, fname, johnfname);
		Statement st2 = vf.createStatement(john, lname, johnlname);
		Statement st3 = vf.createStatement(john, homeTel, johnhomeTel);
		Statement st4 = vf.createStatement(john, email, johnemail);
		Statement st5 = vf.createStatement(micah, fname, micahfname);
		Statement st6 = vf.createStatement(micah, lname, micahlname);
		Statement st7 = vf.createStatement(micah, homeTel, micahhomeTel);
		Statement st8 = vf.createStatement(fei, fname, feifname);
		Statement st9 = vf.createStatement(fei, lname, feilname);
		Statement st10 = vf.createStatement(fei, email, feiemail);
		
		testAdminCon.add(st1, dirgraph);
		testAdminCon.add(st2, dirgraph);
		testAdminCon.add(st3, dirgraph);
		testAdminCon.add(st4, dirgraph);
		testAdminCon.add(st5, dirgraph);
		testAdminCon.add(st6, dirgraph);
		testAdminCon.add(st7, dirgraph);
		testAdminCon.add(st8, dirgraph);
		testAdminCon.add(st9, dirgraph);
		testAdminCon.add(st10, dirgraph);
		
				
		try{
			Assert.assertEquals(10, testAdminCon.size(dirgraph));
		}
		catch(Exception ex){
			logger.error("Failed :", ex);
		}
			
		
		StringBuilder queryBuilder = new StringBuilder();
		
		queryBuilder.append(" PREFIX ad: <http://marklogicsparql.com/addressbook#> ");
		queryBuilder.append(" SELECT ?name ?id ?g ");
		queryBuilder.append(" FROM NAMED  ");
		queryBuilder.append("<").append(dirgraph.stringValue()).append(">");
		queryBuilder.append(" WHERE ");
		queryBuilder.append("  {  ");
		queryBuilder.append(" GRAPH ?g { ?id ad:lastName  ?name .} ");
		queryBuilder.append(" FILTER  EXISTS { GRAPH ?g  {?id ad:email ?email ;  ");
		queryBuilder.append("  ad:firstName ?fname.}");
		queryBuilder.append("  } ");
		queryBuilder.append(" }  ");
		queryBuilder.append(" ORDER BY ?name ");
		
		TupleQuery query = testAdminCon.prepareTupleQuery(queryBuilder.toString());
		TupleQueryResult result = query.evaluate();
		
		String [] epectedPersonresult = {fei.stringValue(), john.stringValue()};
		String [] expectedLnameresult = {feilname.stringValue(), johnlname.stringValue()};
		String [] expectedGraphresult = {dirgraph.stringValue(), dirgraph.stringValue()};
		
		int i = 0;
		try {
			assertThat(result, is(notNullValue()));
			Assert.assertTrue(result.hasNext());
			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertThat(solution.hasBinding("name"), is(equalTo(true)));
				assertThat(solution.hasBinding("id"), is(equalTo(true)));
				assertThat(solution.hasBinding("g"), is(equalTo(true)));
				
				Value idResult = solution.getValue("id");
				Value nameResult = solution.getValue("name");
				Value graphResult = solution.getValue("g");
				
				Assert.assertEquals(idResult.stringValue(),epectedPersonresult[i]);
				Assert.assertEquals(nameResult.stringValue(),expectedLnameresult[i]);
				Assert.assertEquals(graphResult.stringValue(),expectedGraphresult[i]);
				i++;
			}
		}
		finally {
			result.close();
		}
	
	}
	
	// ISSSUE 109
	@Test
	public void testPrepareTupleQuery4() throws Exception{
		
		Statement st1 = vf.createStatement(john, fname, johnfname, dirgraph);
		Statement st2 = vf.createStatement(john, lname, johnlname, dirgraph);
		Statement st3 = vf.createStatement(john, homeTel, johnhomeTel, dirgraph);
		Statement st4 = vf.createStatement(john, email, johnemail, dirgraph);
		Statement st5 = vf.createStatement(micah, fname, micahfname, dirgraph);
		Statement st6 = vf.createStatement(micah, lname, micahlname, dirgraph);
		Statement st7 = vf.createStatement(micah, homeTel, micahhomeTel, dirgraph);
		Statement st8 = vf.createStatement(fei, fname, feifname, dirgraph);
		Statement st9 = vf.createStatement(fei, lname, feilname, dirgraph);
		Statement st10 = vf.createStatement(fei, email, feiemail, dirgraph);
		
		testAdminCon.add(st1);
		testAdminCon.add(st2);
		testAdminCon.add(st3);
		testAdminCon.add(st4);
		testAdminCon.add(st5);
		testAdminCon.add(st6);
		testAdminCon.add(st7);
		testAdminCon.add(st8);
		testAdminCon.add(st9);
		testAdminCon.add(st10);
		
				
		try{
			Assert.assertEquals(10, testAdminCon.size(dirgraph));
		}
		catch(Exception ex){
			logger.error("Failed :", ex);
		}
			
		
		StringBuilder queryBuilder = new StringBuilder(64);
		
		queryBuilder.append("PREFIX ad: <http://marklogicsparql.com/addressbook#> ");
		queryBuilder.append(" SELECT ?person ?firstname ?lastname ?phonenumber");
		queryBuilder.append(" FROM <").append(dirgraph.stringValue()).append(">");
		queryBuilder.append(" WHERE");
		queryBuilder.append(" { ");
		queryBuilder.append("   ?person <#firstName> ?firstname ;");
		queryBuilder.append("           <#lastName> ?lastname. ");
		queryBuilder.append("   OPTIONAL {?person <#homeTel> ?phonenumber .} ");
		queryBuilder.append("   VALUES ?firstname { \"Micah\" \"Fei\" }");
		queryBuilder.append(" } ");
		queryBuilder.append(" ORDER BY ?firstname");
		
		TupleQuery query = testAdminCon.prepareTupleQuery(queryBuilder.toString(),"http://marklogicsparql.com/addressbook");
		TupleQueryResult result = query.evaluate();
		
		String [] epectedPersonresult = {"http://marklogicsparql.com/id#3333", "http://marklogicsparql.com/id#2222"};
		String [] expectedLnameresult = {"Ling", "Dubinko"};
		String [] expectedFnameresult = {"Fei", "Micah"};
		Double [] expectedPhoneresult = {null , 22222222D};
		int i = 0;
		try {
			assertThat(result, is(notNullValue()));
			Assert.assertTrue(result.hasNext());
			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertThat(solution.hasBinding("person"), is(equalTo(true)));
				assertThat(solution.hasBinding("lastname"), is(equalTo(true)));
				Value personResult = solution.getValue("person");
				Value lnameResult = solution.getValue("lastname");
				Value fnameResult = solution.getValue("firstname");
				Literal phoneResult = (Literal) solution.getValue("phonenumber");		
				Assert.assertEquals(epectedPersonresult[i], personResult.stringValue());
				Assert.assertEquals(expectedLnameresult[i], lnameResult.stringValue());
				Assert.assertEquals(expectedFnameresult[i], fnameResult.stringValue());
				Assert.assertEquals(expectedPhoneresult[i],  new Double(phoneResult.doubleValue()) );
				
				i++;
			}
		}
		finally {
			result.close();
		}
	
	}
	
	@Test
	public void testPrepareTupleQueryEmptyResult() throws Exception{
		
		Statement st1 = vf.createStatement(john, fname, johnfname, dirgraph);
		Statement st2 = vf.createStatement(john, homeTel, johnhomeTel, dirgraph);
		Statement st3 = vf.createStatement(micah, fname, micahfname, dirgraph);
		Statement st4 = vf.createStatement(micah, homeTel, micahhomeTel, dirgraph);
		
		testAdminCon.add(st1);
		testAdminCon.add(st2);
		testAdminCon.add(st3);
		testAdminCon.add(st4);
					
		try{
			Assert.assertEquals(4, testAdminCon.size(dirgraph));
		}
		catch(Exception ex){
			logger.error("Failed :", ex);
		}
			
		
		StringBuilder queryBuilder = new StringBuilder(64);
		
		queryBuilder.append("PREFIX ad: <http://marklogicsparql.com/addressbook#> ");
		queryBuilder.append(" SELECT ?person ?p ?o");
		queryBuilder.append(" FROM <").append(dirgraph.stringValue()).append(">");
		queryBuilder.append(" WHERE");
		queryBuilder.append(" { ");
		queryBuilder.append("   ?person <#firstName> ?firstname ;");
		queryBuilder.append("           <#lastName> ?lastname. ");
		queryBuilder.append("   OPTIONAL {?person <#homeTel> ?phonenumber .} ");
		queryBuilder.append("   FILTER NOT EXISTS {?person ?p ?o .}");
		queryBuilder.append(" } ");
		queryBuilder.append(" ORDER BY ?o");
		
		TupleQuery query = testAdminCon.prepareTupleQuery(queryBuilder.toString(),"http://marklogicsparql.com/addressbook");
		TupleQueryResult result = query.evaluate();
		
		assertThat(result, is(notNullValue()));
		Assert.assertFalse(result.hasNext());
	}
	
	@Test
	public void testPrepareGraphQuery1() throws Exception
	{
		Statement st1 = vf.createStatement(john, fname, johnfname, dirgraph);
		Statement st2 = vf.createStatement(john, lname, johnlname, dirgraph);
		Statement st3 = vf.createStatement(john, homeTel, johnhomeTel, dirgraph);
		Statement st4 = vf.createStatement(john, email, johnemail, dirgraph);
		Statement st5 = vf.createStatement(micah, fname, micahfname, dirgraph);
		Statement st6 = vf.createStatement(micah, lname, micahlname, dirgraph);
		Statement st7 = vf.createStatement(micah, homeTel, micahhomeTel, dirgraph);
		Statement st8 = vf.createStatement(fei, fname, feifname, dirgraph);
		Statement st9 = vf.createStatement(fei, lname, feilname, dirgraph);
		Statement st10 = vf.createStatement(fei, email, feiemail, dirgraph);
		
		StatementList<Statement> sL = new StatementList<Statement>(st1);
		sL.add(st2);
		sL.add(st3);
		sL.add(st4);
		sL.add(st5);
		sL.add(st6);
		sL.add(st7);
		sL.add(st8);
		sL.add(st9);
		sL.add(st10);
		
		StatementIterator iter = new StatementIterator(sL);
		testAdminCon.add(new StatementIterable(iter), dirgraph);
		Assert.assertEquals(10, testAdminCon.size(dirgraph));		
			
		StringBuilder queryBuilder = new StringBuilder(128);
		queryBuilder.append(" PREFIX ad: <http://marklogicsparql.com/addressbook#>");
		queryBuilder.append(" CONSTRUCT{ ?person ?p ?o .} ");
		queryBuilder.append(" FROM <http://marklogic.com/dirgraph>");
		queryBuilder.append(" WHERE ");
		queryBuilder.append(" { ");
		queryBuilder.append("   ?person ad:firstName ?firstname ; ");
		queryBuilder.append("           ad:lastName  ?lastname ;  ");
		queryBuilder.append("           ?p ?o . ");
		queryBuilder.append(" } ");
		queryBuilder.append(" order by $person ?p ?o ");
		
		GraphQuery query = testAdminCon.prepareGraphQuery(QueryLanguage.SPARQL, queryBuilder.toString());
		query.setBinding("firstname", vf.createLiteral("Micah"));
		
		GraphQueryResult result = query.evaluate();
		
		Literal [] expectedObjectresult = {micahfname, micahhomeTel, micahlname};
		URI []  expectedPredicateresult = {fname, homeTel, lname};
		int i = 0;
		
		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(true)));
			while (result.hasNext()) {
				Statement st = result.next();
				URI subject = (URI) st.getSubject();
				Assert.assertEquals(subject, micah);
				URI predicate = st.getPredicate();
				Assert.assertEquals(predicate, expectedPredicateresult[i]);
				Value object = st.getObject();
				Assert.assertEquals(object, expectedObjectresult[i]);
				i++;
			}
		}
		finally {
			result.close();
		}
		
		StringBuilder qB = new StringBuilder(128);
		qB.append(" PREFIX ad: <http://marklogicsparql.com/addressbook#>");
		qB.append(" CONSTRUCT{ ?person ?p ?o .} ");
		qB.append(" FROM <http://marklogic.com/dirgraph>");
		qB.append(" WHERE ");
		qB.append(" { ");
		qB.append("   ?person ad:firstname ?firstname ; ");
		qB.append("  ?p ?o . ");
		qB.append("  VALUES ?firstname { \"Fei\" }  ");
		qB.append(" } ");
		qB.append(" order by $person ?p ?o ");
		
		GraphQuery query1 = testAdminCon.prepareGraphQuery(QueryLanguage.SPARQL, qB.toString());
		GraphQueryResult result1 = query1.evaluate();
		assertThat(result1, is(notNullValue()));
		Assert.assertFalse(result1.hasNext());
	}
	
	@Test
	public void testPrepareGraphQuery2() throws Exception
	{
		Statement st1 = vf.createStatement(john, fname, johnfname, dirgraph);
		Statement st2 = vf.createStatement(john, lname, johnlname, dirgraph);
		Statement st3 = vf.createStatement(john, homeTel, johnhomeTel, dirgraph);
		Statement st4 = vf.createStatement(john, email, johnemail, dirgraph);
		Statement st5 = vf.createStatement(micah, fname, micahfname, dirgraph);
		Statement st6 = vf.createStatement(micah, lname, micahlname, dirgraph);
		Statement st7 = vf.createStatement(micah, homeTel, micahhomeTel, dirgraph);
		Statement st8 = vf.createStatement(fei, fname, feifname, dirgraph);
		Statement st9 = vf.createStatement(fei, lname, feilname, dirgraph);
		Statement st10 = vf.createStatement(fei, email, feiemail, dirgraph);
		
		StatementList<Statement> sL = new StatementList<Statement>(st1);
		sL.add(st2);
		sL.add(st3);
		sL.add(st4);
		sL.add(st5);
		sL.add(st6);
		sL.add(st7);
		sL.add(st8);
		sL.add(st9);
		sL.add(st10);
		
		
		StatementIterator iter = new StatementIterator(sL);
		Iteration<Statement, Exception> it = new IteratorIteration<Statement, Exception> (iter);
		testAdminCon.add(it, dirgraph);
		Assert.assertEquals(10, testAdminCon.size(dirgraph));		
			
		StringBuilder queryBuilder = new StringBuilder(128);
		queryBuilder.append(" PREFIX ad: <http://marklogicsparql.com/addressbook#>");
		queryBuilder.append(" PREFIX id:  <http://marklogicsparql.com/id#> ");
		queryBuilder.append(" CONSTRUCT{ <#1111> ad:email ?e .} ");
		queryBuilder.append(" FROM <http://marklogic.com/dirgraph> ");
		queryBuilder.append(" WHERE ");
		queryBuilder.append(" { ");
		queryBuilder.append("  <#1111> ad:lastName ?o; ");
		queryBuilder.append("          ad:email  ?e. ");
		queryBuilder.append(" }  ");
		
		GraphQuery query = testAdminCon.prepareGraphQuery(QueryLanguage.SPARQL, queryBuilder.toString(), "http://marklogicsparql.com/id");
		GraphQueryResult result = query.evaluate();
		
		Literal [] expectedObjectresult = {johnemail};
		URI []  expectedPredicateresult = {email};
		int i = 0;
		
		try {
			assertThat(result, is(notNullValue()));
			while (result.hasNext()) {
				Statement st = result.next();
				URI subject = (URI) st.getSubject();
				Assert.assertEquals(subject, john);
				URI predicate = st.getPredicate();
				Assert.assertEquals(predicate, expectedPredicateresult[i]);
				Value object = st.getObject();
				Assert.assertEquals(object, expectedObjectresult[i]);
				i++;
			}
		}
		finally {
			result.close();
		}
		
	}
	
	@Test
	public void testPrepareGraphQuery3() throws Exception
	{
		Statement st1 = vf.createStatement(john, fname, johnfname, dirgraph);
		Statement st2 = vf.createStatement(john, lname, johnlname, dirgraph);
		Statement st3 = vf.createStatement(john, homeTel, johnhomeTel, dirgraph);
		Statement st4 = vf.createStatement(john, email, johnemail, dirgraph);
		Statement st5 = vf.createStatement(micah, fname, micahfname, dirgraph);
		Statement st6 = vf.createStatement(micah, lname, micahlname, dirgraph);
		Statement st7 = vf.createStatement(micah, homeTel, micahhomeTel, dirgraph);
		Statement st8 = vf.createStatement(fei, fname, feifname, dirgraph);
		Statement st9 = vf.createStatement(fei, lname, feilname, dirgraph);
		Statement st10 = vf.createStatement(fei, email, feiemail, dirgraph);
		
	
		testWriterCon.add(st1);
		testWriterCon.add(st2);
		testWriterCon.add(st3);
		testWriterCon.add(st4);
		testWriterCon.add(st5);
		testWriterCon.add(st6);
		testWriterCon.add(st7);
		testWriterCon.add(st8);
		testWriterCon.add(st9);
		testWriterCon.add(st10);
		
		Assert.assertEquals(10, testAdminCon.size(dirgraph));		
			
		String query = " DESCRIBE <http://marklogicsparql.com/addressbook#firstName> ";
		GraphQuery queryObj = testReaderCon.prepareGraphQuery(query);
			
		GraphQueryResult result = queryObj.evaluate();
		result.hasNext();
		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(false)));
		}
		finally {
			result.close();
		}
	}
	
	@Test
	public void testPrepareGraphQuery4() throws Exception{
		
		Statement st1 = vf.createStatement(john, fname, johnfname);
		Statement st2 = vf.createStatement(john, lname, johnlname);
		Statement st3 = vf.createStatement(john, homeTel, johnhomeTel);
		Statement st4 = vf.createStatement(john, email, johnemail);
		Statement st5 = vf.createStatement(micah, fname, micahfname);
		Statement st6 = vf.createStatement(micah, lname, micahlname);
		Statement st7 = vf.createStatement(micah, homeTel, micahhomeTel);
		Statement st8 = vf.createStatement(fei, fname, feifname);
		Statement st9 = vf.createStatement(fei, lname, feilname);
		Statement st10 = vf.createStatement(fei, email, feiemail);
		
		testWriterCon.add(st1,dirgraph);
		testWriterCon.add(st2,dirgraph);
		testWriterCon.add(st3,dirgraph);
		testWriterCon.add(st4,dirgraph);
		testWriterCon.add(st5,dirgraph);
		testWriterCon.add(st6,dirgraph);
		testWriterCon.add(st7,dirgraph);
		testWriterCon.add(st8,dirgraph);
		testWriterCon.add(st9,dirgraph);
		testWriterCon.add(st10,dirgraph);
		
		Assert.assertEquals(10, testWriterCon.size(dirgraph));
				
		String query = " DESCRIBE  <#3333>  ";
		GraphQuery queryObj = testReaderCon.prepareGraphQuery(query, "http://marklogicsparql.com/id");
			
		GraphQueryResult result = queryObj.evaluate();
		int i = 0;
	
		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(true)));
			while (result.hasNext()) {
				Statement st = result.next();
				URI subject = (URI) st.getSubject();
				Assert.assertNotNull(subject);
				URI predicate = st.getPredicate();
				Assert.assertNotNull(predicate);
				Value object = st.getObject();
				Assert.assertNotNull(object);
				i++;
			}
		}
		finally {
			result.close();
		}
		Assert.assertEquals(3, i);
	
	}
	
	//ISSUE 70
	@Test
	public void testPrepareQuery1() throws Exception {
		testAdminCon.add(MarkLogicRepositoryConnectionTest.class.getResourceAsStream(TEST_DIR_PREFIX + "companies_100.ttl"), "",
				RDFFormat.TURTLE, null);
		Assert.assertEquals(testAdminCon.size(), 1600L);
		
		StringBuilder queryBuilder = new StringBuilder(128);
		queryBuilder.append("PREFIX demor: <http://demo/resource#>");
		queryBuilder.append(" PREFIX demov: <http://demo/verb#>");
		queryBuilder.append(" PREFIX vcard: <http://www.w3.org/2006/vcard/ns#>");
		queryBuilder.append(" SELECT (COUNT(?company) AS ?total)");
		queryBuilder.append(" WHERE { ");
		queryBuilder.append("  ?company a vcard:Organization .");
		queryBuilder.append("  ?company demov:industry ?industry .");
		queryBuilder.append("  ?company vcard:hasAddress/vcard:postal-code ?zip .");
		queryBuilder.append("  ?company vcard:hasAddress/vcard:postal-code ?whatcode ");
		queryBuilder.append(" } ");
		

		Query query = testAdminCon.prepareQuery(QueryLanguage.SPARQL, queryBuilder.toString());
		query.setBinding("whatcode", vf.createLiteral("33333"));
		TupleQueryResult result = null;
        if (query instanceof TupleQuery) {
            result = ((TupleQuery) query).evaluate();
        
        }
		
		try {
			assertThat(result, is(notNullValue()));
		
			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertThat(solution.hasBinding("total"), is(equalTo(true)));
				Value totalResult = solution.getValue("total");
				Assert.assertEquals(vf.createLiteral("12",XMLSchema.UNSIGNED_LONG),totalResult);
				
			}
		}
		finally {
			result.close();
		}
	}
	
	
	@Test
	public void testPrepareQuery2() throws Exception{
		
		Reader ir = new BufferedReader(new InputStreamReader(MarkLogicRepositoryConnectionTest.class.getResourceAsStream(TEST_DIR_PREFIX + "property-paths.ttl")));
		testAdminCon.add(ir, "", RDFFormat.TURTLE);
		
		StringBuilder queryBuilder = new StringBuilder(128);
		queryBuilder.append(" prefix : <http://learningsparql.com/ns/papers#> ");
		queryBuilder.append(" prefix c: <http://learningsparql.com/ns/citations#>");
		queryBuilder.append(" SELECT ?s");
		queryBuilder.append(" WHERE {  ");
		queryBuilder.append(" ?s ^c:cites :paperK2 . ");
		queryBuilder.append(" FILTER (?s != :paperK2)");
		queryBuilder.append(" } ");
		queryBuilder.append(" ORDER BY ?s ");
		
		Query query = testAdminCon.prepareQuery(queryBuilder.toString());
		query.setBinding("whatcode", vf.createLiteral("33333"));
		TupleQueryResult result = null;
        if (query instanceof TupleQuery) {
            result = ((TupleQuery) query).evaluate();
        
        }
		
		try {
			assertThat(result, is(notNullValue()));
		
			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertThat(solution.hasBinding("s"), is(equalTo(true)));
				Value totalResult = solution.getValue("s");
				Assert.assertEquals(vf.createURI("http://learningsparql.com/ns/papers#paperJ"),totalResult);
				
			}
		}
		finally {
			result.close();
		}
	}
	
	@Test
	public void testPrepareQuery3() throws Exception{
	
		Statement st1 = vf.createStatement(john, fname, johnfname);
		Statement st2 = vf.createStatement(john, lname, johnlname);
		Statement st3 = vf.createStatement(john, homeTel, johnhomeTel);
				
		
		testWriterCon.add(st1,dirgraph);
		testWriterCon.add(st2,dirgraph);
		testWriterCon.add(st3,dirgraph);
		
		
		Assert.assertEquals(3, testWriterCon.size(dirgraph));
				
		String query = " DESCRIBE  <http://marklogicsparql.com/id#1111>  ";
		Query queryObj = testReaderCon.prepareQuery(query, "http://marklogicsparql.com/id");
		GraphQueryResult result = null;
		
        if (queryObj instanceof GraphQuery) {
            result = ((GraphQuery) queryObj).evaluate();
        
        }
		

		Literal [] expectedObjectresult = {johnfname, johnlname, johnhomeTel};
		URI []  expectedPredicateresult = {fname, lname, homeTel};
		int i = 0;
	
		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(true)));
			while (result.hasNext()) {
				Statement st = result.next();
				URI subject = (URI) st.getSubject();
				Assert.assertEquals(subject, john);
				URI predicate = st.getPredicate();
				Assert.assertEquals(predicate, expectedPredicateresult[i]);
				Value object = st.getObject();
				Assert.assertEquals(object, expectedObjectresult[i]);
				i++;
			}
		}
		finally {
			result.close();
		}
	}
	
	//ISSUE 70
	@Test
	public void testPrepareQuery4() throws Exception{
		
		URL url = MarkLogicRepositoryConnectionTest.class.getResource(TEST_DIR_PREFIX+"tigers.ttl");
		testAdminCon.add(url, "", RDFFormat.TURTLE);
		Assert.assertEquals(107L, testAdminCon.size());
			
		String query1 = "ASK "+
				"WHERE"+ 
				"{"+
				" ?s <#position> ?o."+
				 "}";

		Query bq = testAdminCon.prepareQuery(query1, "http://marklogic.com/baseball/players");
		bq.setBinding("o", vf.createLiteral("pitcher"));
		boolean result1	= ((BooleanQuery)bq).evaluate();
		Assert.assertTrue(result1);	
		
	}
	
	
	@Test
	public void testPrepareMultipleBaseURI1() throws Exception{
		
	}
	
	@Test
	public void testPrepareMultipleBaseURI2() throws Exception{
		
	}
	
		
	// ISSUE 123
    @Test
    public void testGraphPerms1()
            throws Exception {

        GraphManager gmgr = databaseClient.newGraphManager();
        Resource context = testAdminCon.getValueFactory().createURI("http://marklogic.com/test/graph/permstest");

        String defGraphQuery = "CREATE GRAPH <http://marklogic.com/test/graph/permstest> ";
        MarkLogicUpdateQuery updateQuery = testAdminCon.prepareUpdate(QueryLanguage.SPARQL, defGraphQuery);
        updateQuery.setGraphPerms(gmgr.permission("read-privileged", Capability.READ));
        updateQuery.execute();
        
        String defGraphQuery1 = "INSERT DATA { GRAPH <http://marklogic.com/test/graph/permstest> { <http://marklogic.com/test1> <pp2> \"test\" } }";
        String checkQuery = "ASK WHERE {  GRAPH <http://marklogic.com/test/graph/permstest> {<http://marklogic.com/test> <pp2> \"test\" }}";
        MarkLogicUpdateQuery updateQuery1 = testAdminCon.prepareUpdate(QueryLanguage.SPARQL, defGraphQuery1);
        updateQuery1.execute();
        
        BooleanQuery booleanQuery = testAdminCon.prepareBooleanQuery(QueryLanguage.SPARQL, checkQuery);
        boolean results = booleanQuery.evaluate();
        Assert.assertEquals(true, results);
      
        // add get permissions and verify once those methods are added
    }
    
    //ISSUE 108
	@Test
	public void testAddDelete()
		throws OpenRDFException
	{
		Statement st1 = vf.createStatement(john, fname, johnfname, dirgraph);
		testWriterCon.begin();
		testWriterCon.add(st1);
		testWriterCon.prepareUpdate(QueryLanguage.SPARQL,
				"DELETE DATA {<" + john.stringValue() + "> <" + fname.stringValue() + "> \"" + johnfname.stringValue() + "\"}").execute();
		testWriterCon.commit();

		testWriterCon.exportStatements(null, null, null, false, new RDFHandlerBase() {

			@Override
			public void handleStatement(Statement st)
				throws RDFHandlerException
			{
				assertThat(st, is(not(equalTo(st1))));
			}
		});
	}
	//ISSUE 108, 118
	@Test
	public final void testInsertRemove()
		throws OpenRDFException
	{
		Statement st1 = vf.createStatement(john, homeTel, johnhomeTel);
		testAdminCon.begin();
		testAdminCon.prepareUpdate(
				"INSERT DATA {GRAPH <" + dirgraph.stringValue()+"> { <" + john.stringValue() + "> <" + homeTel.stringValue() + "> \"" + johnhomeTel.doubleValue() + "\"^^<http://www.w3.org/2001/XMLSchema#double>}}").execute();
		
		testAdminCon.remove(st1,dirgraph);
		testAdminCon.commit();
		Assert.assertEquals(0L, testAdminCon.size());
		testAdminCon.exportStatements(null, null, null, false, new RDFHandlerBase() {

			@Override
			public void handleStatement(Statement st)
				throws RDFHandlerException
			{
				assertThat(st, is(not(equalTo(st1))));
			}
		},dirgraph);
	}
	
	//ISSUE 108, 118
	@Test
	public void testInsertDeleteInsertWhere()
		throws OpenRDFException
	{
		Statement st1 = vf.createStatement(john, email, johnemail, dirgraph);
		Statement st2 = vf.createStatement(john, lname, johnlname, dirgraph);
		
		testAdminCon.add(st1);
		testAdminCon.add(st2);
		testAdminCon.begin();
		testAdminCon.prepareUpdate(QueryLanguage.SPARQL,
				"INSERT DATA {GRAPH <" + dirgraph.stringValue()+ "> { <" + john.stringValue() + "> <" + fname.stringValue() + "> \"" + johnfname.stringValue() + "\"} }").execute();
		
		testAdminCon.prepareUpdate(
				"DELETE DATA {GRAPH <" + dirgraph.stringValue()+ "> { <" + john.stringValue() + "> <" + email.stringValue() + "> \"" + johnemail.stringValue() + "\"} }").execute();
		
		String query1 ="PREFIX ad: <http://marklogicsparql.com/addressbook#>"
				+" INSERT {GRAPH <"
				+ dirgraph.stringValue()
				+ "> { <#1111> ad:email \"jsenelson@marklogic.com\"}}"
				+ " where { GRAPH <"+ dirgraph.stringValue()+">{<#1111> ad:lastName  ?name .} } " ;
		
		testAdminCon.prepareUpdate(QueryLanguage.SPARQL,query1, "http://marklogicsparql.com/id").execute();
		testAdminCon.commit();

		Statement expSt = vf.createStatement(john, email, vf.createLiteral("jsnelson@marklogic.com"), dirgraph);
		Assert.assertEquals(3L, testAdminCon.size(dirgraph));
		testAdminCon.exportStatements(null, null, null, false, new RDFHandlerBase() {

			@Override
			public void handleStatement(Statement st)
				throws RDFHandlerException
			{
				logger.debug("St object is :"+st.getObject().stringValue());
				logger.debug("expSt object is :"+expSt.getObject().stringValue());
				assertThat(st, is(equalTo(expSt)));
			}
		});
	}

	@Test
	public void testAddRemoveAdd()
		throws OpenRDFException
	{
		Statement st = vf.createStatement(john, lname, johnlname, dirgraph);
		testAdminCon.add(st);
		testAdminCon.begin();
		testAdminCon.remove(st, dirgraph);
		testAdminCon.add(st);
		testAdminCon.commit();
		Assert.assertFalse(testAdminCon.isEmpty());
	}
	
	@Test
	public void testAddDeleteAdd()
		throws OpenRDFException
	{
		Statement stmt = vf.createStatement(micah, homeTel, micahhomeTel, dirgraph);
		testAdminCon.add(stmt);
		testAdminCon.begin();
		testAdminCon.prepareUpdate(QueryLanguage.SPARQL,
				"DELETE DATA {GRAPH <" + dirgraph.stringValue()+ "> { <" + micah.stringValue() + "> <" + homeTel.stringValue() + "> \"" + micahhomeTel.doubleValue() + "\"^^<http://www.w3.org/2001/XMLSchema#double>} }").execute();
		testAdminCon.add(stmt);
		testAdminCon.commit();
		Assert.assertFalse(testAdminCon.isEmpty());
	}

	@Test
	public void testAddRemoveInsert()
		throws OpenRDFException
	{
		Statement stmt = vf.createStatement(micah, homeTel, micahhomeTel);
		testAdminCon.add(stmt);
		Assert.assertEquals(1, testAdminCon.size());
		testAdminCon.begin();
		testAdminCon.remove(stmt);
		testAdminCon.prepareUpdate(
				"INSERT DATA "+" { <" + micah.stringValue() + "> <#homeTel> \"" + micahhomeTel.doubleValue() + "\"^^<http://www.w3.org/2001/XMLSchema#double>} ","http://marklogicsparql.com/addressbook").execute();
	
		testAdminCon.commit();
		Assert.assertFalse(testAdminCon.isEmpty());
		Assert.assertEquals(1L, testAdminCon.size());
		
	}

	// ISSSUE 106, 118
	@Test
	public void testAddDeleteInsertWhere()
		throws OpenRDFException
	{
		testAdminCon.add(fei,lname,feilname);
		testAdminCon.add(fei, email, feiemail);
		
		testAdminCon.begin();
		testAdminCon.prepareUpdate(
				" DELETE { <" + fei.stringValue() + "> <#email> \"" + feiemail.stringValue() + "\"} "+
          " INSERT { <" + fei.stringValue() + "> <#email> \"fling@marklogic.com\"} where{ ?s <#email> ?o}"
,"http://marklogicsparql.com/addressbook").execute();
	 // Assert.assertTrue(testAdminCon.hasStatement(vf.createStatement(fei, email, vf.createLiteral("fling@marklogic.com")), false));
		
		
	    testAdminCon.commit();

		//Assert.assertTrue(testAdminCon.hasStatement(vf.createStatement(fei, email, vf.createLiteral("fling@marklogic.com")), false));
		Assert.assertFalse(testAdminCon.isEmpty());
	}
	
	@Test
	public void testAddDifferentFormats() throws Exception {
		testAdminCon.add(MarkLogicRepositoryConnectionTest.class.getResourceAsStream(TEST_DIR_PREFIX + "journal.nt"), "",
				RDFFormat.NTRIPLES, dirgraph);
		Assert.assertEquals(testAdminCon.size(), 36L);
		testAdminCon.clear();
		
		testAdminCon.add(new InputStreamReader(MarkLogicRepositoryConnectionTest.class.getResourceAsStream(TEST_DIR_PREFIX + "little.nq")), "",
				RDFFormat.NQUADS);
		Assert.assertEquals(testAdminCon.size(), 9L);
		testAdminCon.clear();
		
		URL url = MarkLogicRepositoryConnectionTest.class.getResource(TEST_DIR_PREFIX+"semantics.trig");
		testAdminCon.add(url, "",RDFFormat.TRIG);
		Assert.assertEquals(testAdminCon.size(), 15L);
		testAdminCon.clear();
		
		File file = new File(MarkLogicRepositoryConnectionTest.class.getResource(TEST_DIR_PREFIX+ "dir.json").getFile());
		testAdminCon.add(file, "", RDFFormat.RDFJSON);
		Assert.assertEquals(testAdminCon.size(), 12L);
		testAdminCon.clear();
		
		Reader fr = new FileReader(new File(MarkLogicRepositoryConnectionTest.class.getResource(TEST_DIR_PREFIX+ "dir.xml").getFile()));
		testAdminCon.add(fr, "", RDFFormat.RDFXML);
		Assert.assertEquals(testAdminCon.size(), 12L);
		testAdminCon.clear();
	}
	
	//ISSUE 110
	@Test
	public void testOpen()
		throws Exception
	{
		Statement stmt = vf.createStatement(micah, homeTel, micahhomeTel, dirgraph);
		testAdminCon.begin();
		assertThat(testAdminCon.isOpen(), is(equalTo(true)));
		assertThat(testWriterCon.isOpen(), is(equalTo(true)));
		testAdminCon.add(stmt);
		testAdminCon.close();
		Assert.assertFalse(testAdminCon.hasStatement(stmt, false, dirgraph));
		testAdminCon.add(stmt);
		Assert.assertEquals(testAdminCon.size(),0);
		assertThat(testAdminCon.isOpen(), is(equalTo(false)));
		assertThat(testWriterCon.isOpen(), is(equalTo(true)));
	}
	
	
	// ISSUE 106
	@Test
	public void testCommit()
		throws Exception
	{
		testAdminCon.begin();
		testAdminCon.add(john, email, johnemail,dirgraph);

		assertTrue("Uncommitted update should be visible to own connection",
				testAdminCon.hasStatement(john, email, johnemail, false, dirgraph));
		assertFalse("Uncommitted update should only be visible to own connection",
				testReaderCon.hasStatement(john, email, johnemail, false, dirgraph));
		assertThat(testWriterCon.size(), is(equalTo(0L)));

		testAdminCon.commit();
		assertThat(testWriterCon.size(), is(equalTo(1L)));
		assertTrue("Repository should contain statement after commit",
				testAdminCon.hasStatement(john, email, johnemail, false, dirgraph));
		assertTrue("Committed update will be visible to all connection",
				testReaderCon.hasStatement(john, email, johnemail, false, dirgraph));
	}
	
	@Test
	public void testSizeRollback()
		throws Exception
	{
		testAdminCon.setIsolationLevel(IsolationLevels.SNAPSHOT);
		assertThat(testAdminCon.size(), is(equalTo(0L)));
		assertThat(testWriterCon.size(), is(equalTo(0L)));
		testAdminCon.begin();
		testAdminCon.add(john, fname, johnfname,dirgraph);
		assertThat(testAdminCon.size(), is(equalTo(1L)));
		assertThat(testWriterCon.size(), is(equalTo(0L)));
		testAdminCon.add(john, fname, feifname);
		assertThat(testAdminCon.size(), is(equalTo(2L)));
		assertThat(testWriterCon.size(), is(equalTo(0L)));
		testAdminCon.rollback();
		assertThat(testAdminCon.size(), is(equalTo(0L)));
		assertThat(testWriterCon.size(), is(equalTo(0L)));
	}

	@Test
	public void testSizeCommit()
		throws Exception
	{
		testAdminCon.setIsolationLevel(IsolationLevels.SNAPSHOT);
		assertThat(testAdminCon.size(), is(equalTo(0L)));
		assertThat(testWriterCon.size(), is(equalTo(0L)));
		testAdminCon.begin();
		testAdminCon.add(john, fname, johnfname,dirgraph);
		assertThat(testAdminCon.size(), is(equalTo(1L)));
		assertThat(testWriterCon.size(), is(equalTo(0L)));
		testAdminCon.add(john, fname, feifname);
		assertThat(testAdminCon.size(), is(equalTo(2L)));
		assertThat(testWriterCon.size(), is(equalTo(0L)));
		testAdminCon.commit();
		assertThat(testAdminCon.size(), is(equalTo(2L)));
		assertThat(testWriterCon.size(), is(equalTo(2L)));
	}
	
	//ISSUE 121
	@Test
	public void  testTransaction() throws Exception{
		
		testAdminCon.begin();
		testAdminCon.commit();
		
		try{
			testAdminCon.commit();
			Assert.assertFalse("Exception was not thrown, when it should have been", 1<2);
		}
		catch (Exception e){
			Assert.assertTrue(e instanceof MarkLogicTransactionException);
		}
		
		try{
			testAdminCon.rollback();
			Assert.assertFalse(2>1);
		}
		catch (Exception e2){
			logger.debug(e2.getMessage());
			Assert.assertTrue(e2 instanceof MarkLogicTransactionException);
		}
		
		
		testAdminCon.begin();
		testAdminCon.prepareUpdate(QueryLanguage.SPARQL,
				"DELETE DATA {GRAPH <" + dirgraph.stringValue()+ "> { <" + micah.stringValue() + "> <" + homeTel.stringValue() + "> \"" + micahhomeTel.doubleValue() + "\"^^<http://www.w3.org/2001/XMLSchema#double>} }").execute();
		testAdminCon.commit();
		Assert.assertTrue(testAdminCon.size()==0);
		
		try{
			testAdminCon.begin();
			testAdminCon.begin();
			Assert.assertFalse("Exception was not thrown, when it should have been", 1<2);
		}
		catch (Exception e){
			Assert.assertTrue(e instanceof MarkLogicTransactionException);
		}
		
	}
	
	// ISSUE 126
	@Test
	public void testClear()
		throws Exception
	{
		testAdminCon.add(john, fname, johnfname,dirgraph);
		testAdminCon.add(john, fname, feifname);
	    assertThat(testAdminCon.hasStatement(null, null, null, false), is(equalTo(true)));
		testAdminCon.clear(dirgraph);
		assertThat(testAdminCon.hasStatement(john, fname, johnfname, false), is(equalTo(false)));
		testAdminCon.clear();
		assertThat(testAdminCon.hasStatement(null, null, null, false), is(equalTo(false)));
	}

	@Test
	public void testAddNullStatements() throws Exception{
		Statement st1 = vf.createStatement(john, fname, null, dirgraph);
		Statement st2 = vf.createStatement(null, lname, johnlname, dirgraph);
		Statement st3 = vf.createStatement(john, homeTel, null );
		Statement st4 = vf.createStatement(john, email, johnemail, null);
		Statement st5 = vf.createStatement(null, null , null, null);
		
		try{
			testAdminCon.add(st1);
			Assert.assertFalse("Exception was not thrown, when it should have been", 1<2);
		}
		catch(Exception e){
			Assert.assertTrue(e instanceof FailedRequestException);
		}
		try{
			testAdminCon.add(st2);
			Assert.assertFalse("Exception was not thrown, when it should have been", 1<2);
		}
		catch(Exception e){
			Assert.assertTrue(e instanceof FailedRequestException);
		}
		try{
			testAdminCon.add(st3);
			Assert.assertFalse("Exception was not thrown, when it should have been", 1<2);
		}
		catch(Exception e){
			Assert.assertTrue(e instanceof FailedRequestException);
		}
		try{
			testAdminCon.add(st5);
			Assert.assertFalse("Exception was not thrown, when it should have been", 1<2);
		}
		catch(Exception e){
			Assert.assertTrue(e instanceof FailedRequestException);
		}
		
		testAdminCon.add(st4);
		Assert.assertEquals(1L,testAdminCon.size());
	}
	
	@Test
	public void testAddMalformedLiteralsDefaultConfig()
		throws Exception
	{
		try {
			testAdminCon.add(
					MarkLogicRepositoryConnectionTest.class.getResourceAsStream(TEST_DIR_PREFIX + "malformed-literals.ttl"),
					"", RDFFormat.TURTLE);
			fail("upload of malformed literals should fail with error in default configuration");
		}
		catch (FailedRequestException e) {
			// ignore, as expected
		}
	}

	@Test
	public void testAddMalformedLiteralsStrictConfig()
		throws Exception
	{
		Set<RioSetting<?>> empty = Collections.emptySet();
		testAdminCon.getParserConfig().setNonFatalErrors(empty);

		try {
			testAdminCon.add(
					MarkLogicRepositoryConnectionTest.class.getResourceAsStream(TEST_DIR_PREFIX + "malformed-literals.ttl"),
					"", RDFFormat.TURTLE);
			fail("upload of malformed literals should fail with error in strict configuration");

		}
		catch (FailedRequestException e) {
			// ingnore, as expected.
		}
	}
	
	//ISSUE 106, 118
	@Test
	public void testRemoveStatements()
		throws Exception
	{
		testAdminCon.begin();
		testAdminCon.add(john, lname, johnlname, dirgraph);
		testAdminCon.add(john, fname, johnfname, dirgraph);
		testAdminCon.add(john, email, johnemail, dirgraph);
		testAdminCon.add(john, homeTel, johnhomeTel, dirgraph);
		testAdminCon.add(micah, lname, micahlname);
		testAdminCon.add(micah, fname, micahfname);
		testAdminCon.add(micah, homeTel, micahhomeTel);
		testAdminCon.commit();
		
		
		testAdminCon.remove(john, null, null);
		/*assertThat(testAdminCon.hasStatement(john, lname, johnlname, false,  dirgraph), is(equalTo(true)));
		assertThat(testAdminCon.hasStatement(john, fname, johnfname, false,  dirgraph), is(equalTo(true)));
		assertThat(testAdminCon.hasStatement(micah, homeTel, micahhomeTel, false), is(equalTo(true)));
		assertThat(testAdminCon.hasStatement(micah, homeTel, micahhomeTel, false), is(equalTo(true)));*/


		testAdminCon.remove(vf.createStatement(null, homeTel, null));
		testAdminCon.remove(vf.createStatement(john, lname, johnlname), dirgraph);

		/*assertThat(testAdminCon.hasStatement(john, homeTel, johnhomeTel, false,  dirgraph), is(equalTo(false)));
		assertThat(testAdminCon.hasStatement(micah, homeTel, micahhomeTel, false), is(equalTo(false)));
		
		assertThat(testAdminCon.hasStatement(john, lname, johnlname, false,  dirgraph), is(equalTo(false)));
		assertThat(testAdminCon.hasStatement(john, fname, johnfname, false,  dirgraph), is(equalTo(true)));*/

		testAdminCon.remove(john, null, null);
//		assertThat(testAdminCon.hasStatement(john, fname, johnfname, false,  dirgraph), is(equalTo(false)));
		assertThat(testAdminCon.isEmpty(), is(equalTo(false)));
		
		testAdminCon.remove(null, null, micahlname);
//		assertThat(testAdminCon.hasStatement(micah, fname, micahfname, false), is(equalTo(true)));
		testAdminCon.remove((URI)null, null, null);
		assertThat(testAdminCon.isEmpty(), is(equalTo(true)));
	}
	
	//ISSUE 106, 118
	@Test
	public void testRemoveStatementCollection()
		throws Exception
	{
		testAdminCon.begin();
		testAdminCon.add(john, lname, johnlname);
		testAdminCon.add(john, fname, johnfname);
		testAdminCon.add(john, email, johnemail);
		testAdminCon.add(john, homeTel, johnhomeTel);
		testAdminCon.add(micah, lname, micahlname, dirgraph);
		testAdminCon.add(micah, fname, micahfname, dirgraph);
		testAdminCon.add(micah, homeTel, micahhomeTel, dirgraph);
		testAdminCon.commit();

//		assertThat(testAdminCon.hasStatement(john, lname, johnlname, false, dirgraph), is(equalTo(false)));
//  	assertThat(testAdminCon.hasStatement(micah, homeTel, micahhomeTel, false, dirgraph), is(equalTo(true)));

		Collection<Statement> c = Iterations.addAll(testAdminCon.getStatements(null, null, null, false),
				new ArrayList<Statement>());

		testAdminCon.remove(c);

//		assertThat(testAdminCon.hasStatement(john, lname, johnlname, false, dirgraph), is(equalTo(false)));
//		assertThat(testAdminCon.hasStatement(micah, homeTel, micahhomeTel, false, dirgraph), is(equalTo(false)));
		assertThat(testAdminCon.isEmpty(), is(equalTo(true)));
	}

	@Test
	public void testRemoveStatementIteration()
		throws Exception
	{
		testAdminCon.begin();
		testAdminCon.add(fei, fname, feifname, dirgraph);
		testAdminCon.add(fei, lname, feilname, dirgraph);
		testAdminCon.add(fei, email, feiemail, dirgraph);
		testAdminCon.commit();

		Assert.assertEquals(3L,testAdminCon.size());

		Iteration<? extends Statement, RepositoryException> iter = testAdminCon.getStatements(null, null,
				null, false);
		
		testAdminCon.remove(iter);
		Assert.assertEquals(0L,testAdminCon.size());
	}
	
	
	// ISSUE 118
	@Test
	public void testGetStatements()
		throws Exception
	{
		testAdminCon.add(john, fname, johnfname);
		testAdminCon.add(john, lname, johnlname);
		testAdminCon.add(john, homeTel, johnhomeTel);
		testAdminCon.add(john, email, johnemail);
		

	//	assertTrue("Repository should contain statement", testAdminCon.hasStatement(john, homeTel, johnhomeTel, false));

		RepositoryResult<Statement> result = testAdminCon.getStatements(null, homeTel, null, false);

		try {
			assertNotNull("Iterator should not be null", result);
			assertTrue("Iterator should not be empty", result.hasNext());

			while (result.hasNext()) {
				Statement st = result.next();
				
				// clarify with Charles if context is null or http://...
				assertNotNull("Statement should not be in a context ", st.getContext());
				assertTrue("Statement predicate should be equal to name ", st.getPredicate().equals(homeTel));
			}
		}
		finally {
			result.close();
		}

		List<Statement> list = Iterations.addAll(testAdminCon.getStatements(null, john, null, false),
				new ArrayList<Statement>());

		assertTrue("List should not be empty", list.isEmpty());
	}

	@Test
	public void testGetStatementsMalformedTypedLiteral()
		throws Exception
	{
	
		Literal invalidIntegerLiteral = vf.createLiteral("four", XMLSchema.INTEGER);
		try {
			testAdminCon.add(micah, homeTel, invalidIntegerLiteral, dirgraph);

			RepositoryResult<Statement> statements = testAdminCon.getStatements(micah, homeTel, null, true);

			assertNotNull(statements);
			assertTrue(statements.hasNext());
			Statement st = statements.next();
			assertTrue(st.getObject() instanceof Literal);
			assertTrue(st.getObject().equals(invalidIntegerLiteral));
		}
		catch (RepositoryException e) {
			// shouldn't happen
			fail(e.getMessage());
		}
	}

	@Test
	public void testGetStatementsMalformedLanguageLiteral()
		throws Exception
	{
		Literal invalidLanguageLiteral = vf.createLiteral("the number four", "japanese");
		try {
			testAdminCon.add(micah, homeTel, invalidLanguageLiteral,dirgraph);

			RepositoryResult<Statement> statements = testAdminCon.getStatements(null, null, null, true,dirgraph);

			assertNotNull(statements);
			assertTrue(statements.hasNext());
			Statement st = statements.next();
			assertTrue(st.getObject() instanceof Literal);
			assertTrue(st.getObject().equals(invalidLanguageLiteral));
		}
		catch (RepositoryException e) {
			e.printStackTrace();
			// shouldn't happen
			fail(e.getMessage());
		}
	}
	
	//ISSUE 26 , 83, 90, 106, 107, 120
	@Test
	public void testGetStatementsInSingleContext()
		throws Exception
	{
		testAdminCon.begin();
		testAdminCon.add(micah, lname, micahlname, dirgraph1);
		testAdminCon.add(micah, fname, micahfname, dirgraph1);
		testAdminCon.add(micah, homeTel, micahhomeTel, dirgraph1);
		
		testAdminCon.add(john, fname, johnfname);
		testAdminCon.add(john, lname, johnlname);
		testAdminCon.add(john, homeTel, johnhomeTel);
		
		
		Assert.assertEquals(3, testAdminCon.size(dirgraph1));
		Assert.assertEquals(6, testAdminCon.size(null));
		Assert.assertNotEquals(0, testAdminCon.size(vf.createURI(":asd")));
		Assert.assertEquals(0, testAdminCon.size(dirgraph));	
		Assert.assertEquals(6, testAdminCon.size());
		
	
		testAdminCon.add(dirgraph, vf.createURI("http://TYPE"), vf.createLiteral("Directory Graph"));
		testAdminCon.commit();		
	
		/*assertTrue("Repository should contain statement", testAdminCon.hasStatement(john, homeTel, johnhomeTel, false));
		assertTrue("Repository should contain statement in dirgraph1",
				testAdminCon.hasStatement(micah, lname, micahlname, false, dirgraph1));
		assertFalse("Repository should not contain statement in context2",
				testAdminCon.hasStatement(micah, lname, micahlname, false, dirgraph));*/

		// Check handling of getStatements without context IDs
		RepositoryResult<Statement> result = testAdminCon.getStatements(micah, lname, null, false);
		try {
			while (result.hasNext()) {
				Statement st = result.next();
				assertThat(st.getSubject(), is(equalTo((Resource)micah)));
				assertThat(st.getPredicate(), is(equalTo(lname)));
				assertThat(st.getObject(), is(equalTo((Value)micahlname)));
				assertThat(st.getContext(), is(equalTo((Resource)null)));
			}
		}
		finally {
			result.close();
		}

		// Check handling of getStatements with a known context ID
		result = testAdminCon.getStatements(null, null, null, false, dirgraph);
		try {
			while (result.hasNext()) {
				Statement st = result.next();
				assertThat(st.getContext(), is(equalTo((Resource)dirgraph)));
			}
		}
		finally {
			result.close();
		}

		// Check handling of getStatements with an unknown context ID
		result = testAdminCon.getStatements(null, null, null, false, vf.createURI("unknownContext"));
		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(false)));
		}
		finally {
			result.close();
		}

		List<Statement> list = Iterations.addAll(testAdminCon.getStatements(null, lname, null, false, dirgraph),
				new ArrayList<Statement>());
		assertNotNull("List should not be null", list);
		assertFalse("List should not be empty", list.isEmpty());
	}
	
	//ISSUE 82, 127
	@Test
	public void testGetStatementsInMultipleContexts()
		throws Exception
	{
		// get all statements with no contexts
		

		URI ur = vf.createURI("http://abcd");
		logger.debug("Size of abcd is"+ testAdminCon.size(ur));
		
		CloseableIteration<? extends Statement, RepositoryException> iter1 = testAdminCon.getStatements(null, null, null, false, null);

		try {
			int count = 0;
			while (iter1.hasNext()) {
				iter1.next();
				count++;
			}
			assertEquals("there should be 0 statements", 0 , count);
		}
		finally {
			iter1.close();
			iter1 = null;
		}
		
		testAdminCon.getStatements(null, homeTel, null, false);
		testAdminCon.begin();
		testAdminCon.add(micah, lname, micahlname, dirgraph1);
		testAdminCon.add(micah, fname, micahfname, dirgraph1);
		testAdminCon.add(micah, homeTel, micahhomeTel, dirgraph1);
		testAdminCon.commit();

		// get statements with either no context or dirgraph1
		CloseableIteration<? extends Statement, RepositoryException> iter = testAdminCon.getStatements(null, null,
				null, false, null , dirgraph1);

		try {
			int count = 0;
			while (iter.hasNext()) {
				count++;
				Statement st = iter.next();
				assertThat(st.getContext(), anyOf(is(equalTo((Resource)dirgraph1))));
			}

			assertEquals("there should be three statements", 3, count);
		}
		finally {
			iter.close();
			iter = null;
		}
		
		iter = testAdminCon.getStatements(null, null, null, false, dirgraph1, dirgraph);

		try {
			int count = 0;
			while (iter.hasNext()) {
				count++;
				Statement st = iter.next();
				assertThat(st.getContext(), anyOf(is(equalTo((Resource)dirgraph)), is(equalTo((Resource)dirgraph1))));
			}
			assertEquals("there should be two statements", 3 , count);
		}
		finally {
			iter.close();
			iter = null;
		}
		

		// get all statements with unknownContext or context2.
		URI unknownContext = testAdminCon.getValueFactory().createURI("http://unknownContext");
		iter = testAdminCon.getStatements(null, null, null, false, unknownContext, dirgraph1);

		try {
			int count = 0;
			while (iter.hasNext()) {
				iter.next();
				count++;
			}
			assertEquals("there should be three statements", 3, count);
		}
		finally {
			iter.close();
			iter = null;
		}

		// add statements to context1
		testAdminCon.begin();
		testAdminCon.add(john, fname, johnfname, dirgraph);
		testAdminCon.add(john, lname, johnlname, dirgraph);
		testAdminCon.add(john, homeTel, johnhomeTel, dirgraph);
		testAdminCon.commit();
		
		// get all statements with dirgraph1 or dirgraph.  dirgraph1 and
		// dirgraph1 are both known in the store because they have been 
		// created through the store's own value vf.
		
		// get statements with either no context or dirgraph
		iter = testAdminCon.getStatements(null, null, null, false, null, dirgraph);
		try {
			assertThat(iter, is(notNullValue()));
			assertThat(iter.hasNext(), is(equalTo(true)));
			int count = 0;
			while (iter.hasNext()) {
				count++;
				Statement st = iter.next();
				assertThat(st.getContext(), anyOf(is(nullValue(Resource.class)), is(equalTo((Resource)dirgraph)) ));
			}
			assertEquals("there should be four statements", 3, count);
		}
		finally {
			iter.close();
			iter = null;
		}

		// get all statements with dirgraph or dirgraph1
		iter = testAdminCon.getStatements(null, null, null, false, dirgraph, dirgraph1);

		try {
			int count = 0;
			while (iter.hasNext()) {
				count++;
				Statement st = iter.next();
				assertThat(st.getContext(),
						anyOf(is(equalTo((Resource)dirgraph)), is(equalTo((Resource)dirgraph1))));
			}
			assertEquals("there should be 6 statements", 6, count);
		}
		finally {
			iter.close();
			iter = null;
		}
		
}
	

	
	@Test
	public void testPagination() throws Exception{
		
		URL url = MarkLogicRepositoryConnectionTest.class.getResource(TEST_DIR_PREFIX+"tigers.ttl");
		testAdminCon.add(url, "", RDFFormat.TURTLE, graph1);
		StringBuilder queryBuilder = new StringBuilder(128);
		queryBuilder.append(" PREFIX  bb: <http://marklogic.com/baseball/players#> ");
		queryBuilder.append(" PREFIX  r: <http://marklogic.com/baseball/rules#> ");
		queryBuilder.append(" SELECT ?id ?lastname  ");
		queryBuilder.append("  {  ");
		queryBuilder.append(" ?id bb:lastname ?lastname. ");
		queryBuilder.append(" } ");
		queryBuilder.append(" ORDER BY ?lastname");
		
		Query query = testAdminCon.prepareQuery(queryBuilder.toString());
		
		TupleQueryResult result1 = ((MarkLogicTupleQuery) query).evaluate(1,2);
		String [] expLname = {"Ausmus","Avila","Bernard","Cabrera","Carrera","Castellanos","Holaday","Joyner","Lamont","Nathan","Verlander"};
		String [] expID ={"http://marklogic.com/baseball/players#157", "http://marklogic.com/baseball/players#120", "http://marklogic.com/baseball/players#130", "http://marklogic.com/baseball/players#123", "http://marklogic.com/baseball/players#131", "http://marklogic.com/baseball/players#124", "http://marklogic.com/baseball/players#121", "http://marklogic.com/baseball/players#159", "http://marklogic.com/baseball/players#158", "http://marklogic.com/baseball/players#107","http://marklogic.com/baseball/players#119"};
		int i =0;
		while (result1.hasNext()) {
			BindingSet solution = result1.next();
			assertThat(solution.hasBinding("lastname"), is(equalTo(true)));
			Value totalResult = solution.getValue("lastname");
			Assert.assertEquals(expLname[i],totalResult.stringValue());
			i++;
		}
		Assert.assertEquals(2, i);
		
		
		TupleQueryResult result2 = ((MarkLogicTupleQuery) query).evaluate(1,0);
		while (result2.hasNext()) {
			BindingSet solution = result2.next();
			assertThat(solution.hasBinding("lastname"), is(equalTo(true)));
			Value totalResult = solution.getValue("lastname");
			Assert.assertEquals(expLname[i],totalResult.stringValue());
			logger.debug("String values : "+ expLname[i] );
			i++;
		}
		
		
		try{
			TupleQueryResult result3 = ((MarkLogicTupleQuery) query).evaluate(0,0);
			Assert.assertTrue(2>1);
		}
		catch(Exception e){
			logger.debug(e.getMessage());
			Assert.assertTrue(e instanceof IllegalArgumentException);
		}
		
		
		try{
			TupleQueryResult result3 = ((MarkLogicTupleQuery) query).evaluate(-1,-1);
			Assert.assertTrue(2>1);
		}
		catch(Exception e){
			logger.debug(e.getMessage());
			Assert.assertTrue(e instanceof IllegalArgumentException);
		}
		
		try{
			TupleQueryResult result3 = ((MarkLogicTupleQuery) query).evaluate(2,-1);
			Assert.assertTrue(2>1);
		}
		catch(Exception e){
			logger.debug(e.getMessage());
			Assert.assertTrue(e instanceof IllegalArgumentException);
		}
		
		try{
			TupleQueryResult result3 = ((MarkLogicTupleQuery) query).evaluate(-2,2);
			Assert.assertTrue(2>1);
		}
		catch(Exception e){
			logger.debug(e.getMessage());
			Assert.assertTrue(e instanceof IllegalArgumentException);
		}
		
		i = 0;
		TupleQueryResult result4 = ((MarkLogicTupleQuery) query).evaluate(11,2);
		while (result4.hasNext()) {
			BindingSet solution = result4.next();
			assertThat(solution.hasBinding("lastname"), is(equalTo(true)));
			Value totalResult = solution.getValue("lastname");
			Assert.assertEquals(expLname[11-i-1],totalResult.stringValue());
			
			i++;
		}       
        Assert.assertEquals(1L, i);
	}
	
	// ISSUE 72
	@Test
	public void  testPrepareNonSparql() throws Exception{
		
		URL url = MarkLogicRepositoryConnectionTest.class.getResource(TEST_DIR_PREFIX+"tigers.ttl");
		testAdminCon.add(url, "", RDFFormat.TURTLE, graph1);
		Assert.assertEquals(107L, testAdminCon.size());
			
		String query1 = "ASK "+
				"WHERE"+ 
				"{"+
				" ?s <#position> ?o."+
				 "}";

		try{
			 testAdminCon.prepareGraphQuery(QueryLanguage.SERQL, query1, "http://marklogic.com/baseball/players").evaluate();
			 Assert.assertFalse("Exception was not thrown, when it should have been", 1<2);
		}
		catch(UnsupportedQueryLanguageException ex){
			Assert.assertEquals("Unsupported query language SeRQL", ex.getMessage());
		}
		
		try{
			
			testAdminCon.prepareTupleQuery(QueryLanguage.SERQO, query1).evaluate(1,2);
			Assert.assertFalse("Exception was not thrown, when it should have been", 1<2);
		}
		catch(UnsupportedQueryLanguageException ex1){
			Assert.assertEquals("Unsupported query language SeRQO", ex1.getMessage());
		}
		try{
			testAdminCon.prepareBooleanQuery(QueryLanguage.SERQL, query1).evaluate();
			Assert.assertFalse("Exception was not thrown, when it should have been", 1<2);
		}
		catch(UnsupportedQueryLanguageException ex1){
			Assert.assertEquals("Unsupported query language SeRQL", ex1.getMessage());
		}
		try{
			testAdminCon.prepareUpdate(QueryLanguage.SERQO, query1);
			Assert.assertFalse("Exception was not thrown, when it should have been", 1<2);
		}
		catch(UnsupportedQueryLanguageException ex1){
			Assert.assertEquals("Unsupported query language SeRQO", ex1.getMessage());
		}
			
		try{
			testAdminCon.prepareQuery(QueryLanguage.SERQL, query1);
			Assert.assertFalse("Exception was not thrown, when it should have been", 1<2);			
		}
		catch(UnsupportedQueryLanguageException ex1){
			Assert.assertEquals("Unsupported query language SeRQL", ex1.getMessage());
		}
		
	}
	
	// ISSUE 73
	@Test
	public void  testPrepareInvalidSparql() throws Exception{
		Statement st1 = vf.createStatement(john, fname, johnfname);
		testWriterCon.add(st1,dirgraph);
		Assert.assertEquals(1, testWriterCon.size(dirgraph));
				
		String query = " DESCRIBE  <http://marklogicsparql.com/id#1111>  ";
		
		try{
			boolean tq = testReaderCon.prepareBooleanQuery(query, "http://marklogicsparql.com/id").evaluate();
			
		}
		catch(IllegalArgumentException ex1){
			Assert.assertEquals("", ex1.getMessage());
		}
		
		
		String query1 = "ASK {"+
				"{"+
				" ?s <#position> ?o."+
				 "}";
		try{
			boolean tq = testReaderCon.prepareBooleanQuery(query1, "http://marklogicsparql.com/id").evaluate();
			
		}
		catch(MalformedQueryException ex1){
			Assert.assertEquals("", ex1.getMessage());
		}
		
	}
	
	@Test
	public void  testUnsupportedIsolationLevel() throws Exception{
		Assert.assertEquals(IsolationLevels.SNAPSHOT, testAdminCon.getIsolationLevel());
		
		testAdminCon.begin();
		testAdminCon.add(john, fname, johnfname);
	//	assertThat(testAdminCon.hasStatement(john, fname, johnfname, false), is(equalTo(true)));
	//	assertThat(testWriterCon.hasStatement(john, fname, johnfname, false), is(equalTo(false)));
		testAdminCon.commit();
	//	assertThat(testAdminCon.hasStatement(john, fname, johnfname, false), is(equalTo(true)));
	//	assertThat(testWriterCon.hasStatement(john, fname, johnfname, false), is(equalTo(true)));
		try{
			testAdminCon.setIsolationLevel(IsolationLevels.SNAPSHOT_READ);
			
		}
		catch(Exception e){
			Assert.assertTrue(e instanceof IllegalStateException);
		}
		
	}
	
	// ISSUE - 84 
	@Test
	public void testNoUpdateRole() throws Exception{
		try{
			testAdminCon.prepareUpdate("DROP GRAPH <abc>").execute();
			Assert.assertTrue(false);
		}
		catch(Exception e){
			Assert.assertTrue(e instanceof UpdateExecutionException);
		}
		
		try{
			testReaderCon.prepareUpdate("CREATE GRAPH <abc>").execute();
			Assert.assertTrue(false);
		}
		catch(Exception e){
			Assert.assertTrue(e instanceof UpdateExecutionException);
		}
	}

	//ISSUE 112
	@Test
	public void testRuleSets1() throws Exception{
		
		testAdminCon.add(micah, lname, micahlname, dirgraph1);
		testAdminCon.add(micah, fname, micahfname, dirgraph1);
		testAdminCon.add(micah, developPrototypeOf, semantics, dirgraph1);
		testAdminCon.add(micah, type, sEngineer, dirgraph1);
		testAdminCon.add(micah, worksFor, ml, dirgraph1);
		
		testAdminCon.add(john, fname, johnfname, dirgraph1);
		testAdminCon.add(john, lname, johnlname, dirgraph1);
		testAdminCon.add(john, writeFuncSpecOf, inference, dirgraph1);
		testAdminCon.add(john, type, lEngineer, dirgraph1);
		testAdminCon.add(john, worksFor, ml, dirgraph1);
		
		testAdminCon.add(writeFuncSpecOf, eqProperty, design, dirgraph1);
		testAdminCon.add(developPrototypeOf, eqProperty, design, dirgraph1);
		testAdminCon.add(design, eqProperty, develop, dirgraph1);
		
		// 
		String query = "select  (count (?s)  as ?totalcount) where {?s ?p ?o .} ";
		TupleQuery tupleQuery =  testAdminCon.prepareTupleQuery(QueryLanguage.SPARQL, query);
		((MarkLogicQuery) tupleQuery).setRulesets(SPARQLRuleset.EQUIVALENT_CLASS);
		TupleQueryResult result = tupleQuery.evaluate();
		
	try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(true)));
			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertThat(solution.hasBinding("totalcount"), is(equalTo(true)));
				Value count = solution.getValue("totalcount");
				Assert.assertEquals(15, Integer.parseInt(count.stringValue()));
			}
		}
		finally {
			result.close();
		}
	
		TupleQuery tupleQuery1 =  testAdminCon.prepareTupleQuery(QueryLanguage.SPARQL, query);
		((MarkLogicQuery) tupleQuery1).setRulesets(SPARQLRuleset.EQUIVALENT_PROPERTY);
		TupleQueryResult result1 = tupleQuery1.evaluate();
		
		try {
			assertThat(result1, is(notNullValue()));
			assertThat(result1.hasNext(), is(equalTo(true)));
			while (result1.hasNext()) {
				BindingSet solution = result1.next();
				assertThat(solution.hasBinding("totalcount"), is(equalTo(true)));
				Value count = solution.getValue("totalcount");
				Assert.assertEquals(23,Integer.parseInt(count.stringValue()));
			}
		}
		finally {
			result1.close();
		}
		
			
		TupleQuery tupleQuery2  =  testAdminCon.prepareTupleQuery(QueryLanguage.SPARQL, query);
		tupleQuery2.setIncludeInferred(true);
		TupleQueryResult result2 = tupleQuery2.evaluate();
		
		try {
			assertThat(result2, is(notNullValue()));
			assertThat(result2.hasNext(), is(equalTo(true)));
			while (result2.hasNext()) {
				BindingSet solution = result2.next();
				assertThat(solution.hasBinding("totalcount"), is(equalTo(true)));
				Value count = solution.getValue("totalcount");
				Assert.assertEquals(13, Integer.parseInt(count.stringValue()));
			}
		}
		finally {
			result2.close();
		}
	}
	
	@Test
	public void testRuleSets2() throws Exception{
		
		testAdminCon.add(micah, lname, micahlname, dirgraph1);
		testAdminCon.add(micah, fname, micahfname, dirgraph1);
		testAdminCon.add(micah, developPrototypeOf, semantics, dirgraph1);
		testAdminCon.add(micah, type, sEngineer, dirgraph1);
		testAdminCon.add(micah, worksFor, ml, dirgraph1);
		
		testAdminCon.add(john, fname, johnfname,dirgraph);
		testAdminCon.add(john, lname, johnlname,dirgraph);
		testAdminCon.add(john, writeFuncSpecOf, inference, dirgraph);
		testAdminCon.add(john, type, lEngineer, dirgraph);
		testAdminCon.add(john, worksFor, ml, dirgraph);
		
		testAdminCon.add(writeFuncSpecOf, subProperty, design, dirgraph1);
		testAdminCon.add(developPrototypeOf, subProperty, design, dirgraph1);
		testAdminCon.add(design, subProperty, develop, dirgraph1);
		
		testAdminCon.add(lEngineer, subClass, engineer, dirgraph1);
		testAdminCon.add(sEngineer, subClass, engineer, dirgraph1);
		testAdminCon.add(engineer, subClass, employee, dirgraph1);
		
		String query = "select (count (?s)  as ?totalcount)  where {?s ?p ?o .} ";
		TupleQuery tupleQuery =  testAdminCon.prepareTupleQuery(QueryLanguage.SPARQL, query);
		((MarkLogicQuery) tupleQuery).setRulesets(SPARQLRuleset.RDFS_PLUS_FULL);
		TupleQueryResult result = tupleQuery.evaluate();
		
		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(true)));
			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertThat(solution.hasBinding("totalcount"), is(equalTo(true)));
				Value count = solution.getValue("totalcount");
				Assert.assertEquals(374, count.stringValue());
			}
		}
		finally {
			result.close();
		}
	
		tupleQuery =  testAdminCon.prepareTupleQuery(QueryLanguage.SPARQL, query);
		((MarkLogicQuery) tupleQuery).setRulesets(SPARQLRuleset.EQUIVALENT_CLASS);
		result = tupleQuery.evaluate();
		
		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(true)));
			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertThat(solution.hasBinding("totalcount"), is(equalTo(true)));
				Value count = solution.getValue("totalcount");
				Assert.assertEquals(count.stringValue(), 18);
			}
		}
		finally {
			result.close();
		}
		
		tupleQuery =  testAdminCon.prepareTupleQuery(QueryLanguage.SPARQL, query);
		tupleQuery.setIncludeInferred(true);
		result = tupleQuery.evaluate();
		
		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(true)));
			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertThat(solution.hasBinding("totalcount"), is(equalTo(true)));
				Value count = solution.getValue("totalcount");
				Assert.assertEquals(count.stringValue(), 16);
			}
		}
		finally {
			result.close();
		}
	}
	
	@Test
	public void testConstrainingQueries() throws Exception{
		testAdminCon.begin();
		testAdminCon.add(micah, lname, micahlname, dirgraph1);
		testAdminCon.add(micah, fname, micahfname, dirgraph1);
		testAdminCon.add(micah, homeTel, micahhomeTel, dirgraph1);
		
		testAdminCon.add(john, fname, johnfname);
		testAdminCon.add(john, lname, johnlname);
		testAdminCon.add(john, homeTel, johnhomeTel);
					
		testAdminCon.commit();
		
        String query1 = "ASK WHERE {?s ?p \"Micah\" .}";
        String query2 = "SELECT ?s ?p ?o  WHERE {?s ?p ?o .} ORDER by ?o";

        // case one, rawcombined
        String combinedQuery =
                "{\"search\":" +
                        "{\"qtext\":\"2222\"}}";
        String negCombinedQuery =
                "{\"search\":" +
                        "{\"qtext\":\"John\"}}";

        
        RawCombinedQueryDefinition rawCombined = qmgr.newRawCombinedQueryDefinition(new StringHandle().with(combinedQuery).withFormat(Format.JSON));
        RawCombinedQueryDefinition negRawCombined = qmgr.newRawCombinedQueryDefinition(new StringHandle().with(negCombinedQuery).withFormat(Format.JSON));

        MarkLogicBooleanQuery askQuery = (MarkLogicBooleanQuery) testAdminCon.prepareBooleanQuery(QueryLanguage.SPARQL,query1);
        askQuery.setConstrainingQueryDefinition(rawCombined);
        Assert.assertEquals(true, askQuery.evaluate());

           
        MarkLogicTupleQuery tupleQuery =  testAdminCon.prepareTupleQuery(QueryLanguage.SPARQL, query2);
        tupleQuery.setConstrainingQueryDefinition(negRawCombined);
        TupleQueryResult result = tupleQuery.evaluate();
		
	try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(true)));
			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertThat(solution.hasBinding("s"), is(equalTo(true)));
				Value fname = solution.getValue("o");
				Assert.assertEquals("John", fname.stringValue());
			}
		}
		finally {
			result.close();
		}
	}
	
	// ISSUE 124
	@Test
    public void testStructuredQuery() throws Exception {
	 
	 setupData();
     StructuredQueryBuilder qb = new StructuredQueryBuilder();
     QueryDefinition structuredDef = qb.build(qb.term("Second"));
        
     String posQuery = "ASK WHERE {<http://example.org/r9929> ?p ?o .}";
     String negQuery = "ASK WHERE {<http://example.org/r9928> ?p ?o .}";
       
     MarkLogicBooleanQuery askQuery = (MarkLogicBooleanQuery) testAdminCon.prepareBooleanQuery(QueryLanguage.SPARQL,posQuery);
     askQuery.setConstrainingQueryDefinition(structuredDef);
     Assert.assertEquals(true, askQuery.evaluate());
     
     MarkLogicBooleanQuery askQuery1 = (MarkLogicBooleanQuery) testAdminCon.prepareBooleanQuery(QueryLanguage.SPARQL,negQuery);
     askQuery1.setConstrainingQueryDefinition(structuredDef);
     Assert.assertEquals(false, askQuery1.evaluate());
     
     askQuery = (MarkLogicBooleanQuery) testAdminCon.prepareBooleanQuery(QueryLanguage.SPARQL,posQuery);
     askQuery.setConstrainingQueryDefinition(null);
     Assert.assertEquals(true, askQuery.evaluate());
     
     askQuery1 = (MarkLogicBooleanQuery) testAdminCon.prepareBooleanQuery(QueryLanguage.SPARQL,negQuery);
     askQuery.setConstrainingQueryDefinition(null);
     Assert.assertEquals(true, askQuery1.evaluate());
   }
   
	// ISSUE 124
   @Test
    public void testStringQuery() throws Exception {
	   setupData();
        StringQueryDefinition stringDef = qmgr.newStringDefinition().withCriteria("First");
                
        String posQuery = "ASK WHERE {<http://example.org/r9928> ?p ?o .}";
        String negQuery = "ASK WHERE {<http://example.org/r9929> ?p ?o .}";
        MarkLogicBooleanQuery askQuery = (MarkLogicBooleanQuery) testAdminCon.prepareBooleanQuery(QueryLanguage.SPARQL,posQuery);
        askQuery.setConstrainingQueryDefinition(stringDef);
        Assert.assertEquals(true, askQuery.evaluate());
        
        MarkLogicBooleanQuery askQuery1 = (MarkLogicBooleanQuery) testAdminCon.prepareBooleanQuery(QueryLanguage.SPARQL,negQuery);
        askQuery1.setConstrainingQueryDefinition(stringDef);
        Assert.assertEquals(false, askQuery1.evaluate());
        
        askQuery = (MarkLogicBooleanQuery) testAdminCon.prepareBooleanQuery(QueryLanguage.SPARQL,posQuery);
        askQuery.setConstrainingQueryDefinition(null);
        Assert.assertEquals(true, askQuery.evaluate());
        
        askQuery1 = (MarkLogicBooleanQuery) testAdminCon.prepareBooleanQuery(QueryLanguage.SPARQL,negQuery);
        askQuery1.setConstrainingQueryDefinition(null);
        Assert.assertEquals(true, askQuery1.evaluate());
       
    }
	
 
   private void setupData() {
	   String tripleDocOne = 
			   "<semantic-document>\n" +
                "<title>First Title</title>\n" +
                "<size>100</size>\n" +
                "<sem:triples xmlns:sem=\"http://marklogic.com/semantics\">" +
                "<sem:triple><sem:subject>http://example.org/r9928</sem:subject>" +
                "<sem:predicate>http://example.org/p3</sem:predicate>" +
                "<sem:object datatype=\"http://www.w3.org/2001/XMLSchema#int\">1</sem:object></sem:triple>" +
                "</sem:triples>\n" +
                "</semantic-document>";

        String tripleDocTwo = 
        		"<semantic-document>\n" +
                "<title>Second Title</title>\n" +
                "<size>500</size>\n" +
                "<sem:triples xmlns:sem=\"http://marklogic.com/semantics\">" +
                "<sem:triple><sem:subject>http://example.org/r9929</sem:subject>" +
                "<sem:predicate>http://example.org/p3</sem:predicate>" +
                "<sem:object datatype=\"http://www.w3.org/2001/XMLSchema#int\">2</sem:object></sem:triple>" +
                "</sem:triples>\n" +
                "</semantic-document>";

        XMLDocumentManager docMgr = databaseClient.newXMLDocumentManager();
        docMgr.write("/directory1/doc1.xml", new StringHandle().with(tripleDocOne));
        docMgr.write("/directory2/doc2.xml", new StringHandle().with(tripleDocTwo));
    }
	   
	//ISSUE 125
   @Test
	public void testAutoCommit() throws Exception{
		
		testAdminCon.setAutoCommit(false);
		testAdminCon.add(micah, lname, micahlname, dirgraph1);
		testAdminCon.add(micah, fname, micahfname, dirgraph1);
		
		String query1 = "ASK WHERE {?s ?p \"Micah\" .}";
	    MarkLogicBooleanQuery askQuery = (MarkLogicBooleanQuery) testAdminCon.prepareBooleanQuery(QueryLanguage.SPARQL,query1);
	    Assert.assertEquals(false, askQuery.evaluate());
	    
	    testAdminCon.begin();
	    testAdminCon.add(micah, fname, micahfname, dirgraph1);
		testAdminCon.commit();
		
		askQuery = (MarkLogicBooleanQuery) testAdminCon.prepareBooleanQuery(QueryLanguage.SPARQL,query1);
	    Assert.assertEquals(true, askQuery.evaluate());
	    Assert.assertEquals(1, testAdminCon.size());
	    
	    testAdminCon.setAutoCommit(true);
	    testAdminCon.add(micah, lname, micahlname, dirgraph1);
		String query2= "ASK WHERE {?s ?p \"Dubinko\" .}";
	    askQuery = (MarkLogicBooleanQuery) testAdminCon.prepareBooleanQuery(QueryLanguage.SPARQL,query2);
	    Assert.assertEquals(true, askQuery.evaluate());
		
	}
}