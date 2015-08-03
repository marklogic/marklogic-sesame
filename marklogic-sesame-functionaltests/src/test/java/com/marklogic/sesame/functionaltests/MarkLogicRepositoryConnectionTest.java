package com.marklogic.sesame.functionaltests;

import static org.hamcrest.core.AnyOf.anyOf;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import info.aduna.iteration.CloseableIteration;

import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.BindingSet;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.repository.config.RepositoryConfigException;
import org.openrdf.repository.config.RepositoryFactory;
import org.openrdf.rio.RDFFormat;
import org.openrdf.sail.SailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.marklogic.semantics.sesame.MarkLogicRepository;
import com.marklogic.semantics.sesame.MarkLogicRepositoryConnection;
import com.marklogic.semantics.sesame.client.MarkLogicClient;
import com.marklogic.semantics.sesame.config.MarkLogicRepositoryConfig;
import com.marklogic.semantics.sesame.config.MarkLogicRepositoryFactory;
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
	private static int uberPort = 8000;

	
	protected static MarkLogicRepository testAdminRepository;
	protected static MarkLogicRepository testReaderRepository;
	protected static MarkLogicRepository testWriterRepository;
	protected static MarkLogicRepositoryConnection testAdminCon;
	protected static MarkLogicRepositoryConnection testReaderCon;
	protected static MarkLogicRepositoryConnection testWriterCon;
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());
	protected ValueFactory vf;
	protected URI graph1;
	protected URI graph2;
	protected URI dirgraph;
	
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
	
	private static final String ID = "id";
	private static final String ADDRESS = "addressbook";
	protected static final String NS = "http://marklogicsparql.com/";
	
	@BeforeClass
	public static void initialSetup() throws Exception {
		
	/*	setupJavaRESTServer(dbName, fNames[0], restServer, restPort);
		setupAppServicesConstraint(dbName);
		enableCollectionLexicon(dbName);
		enableTripleIndex(dbName);
		
		createUserRolesWithPrevilages("test-eval", "xdbc:eval", "xdbc:eval-in", "xdmp:eval-in", "any-uri", "xdbc:invoke");
		createRESTUser("reader", "reader", "test-eval", "rest-reader");
		createRESTUser("writer", "writer", "test-eval", "rest-writer");*/
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
		
		john = vf.createURI(NS+ID+"#1111");
		micah = vf.createURI(NS+ID+"#2222");
		fei = vf.createURI(NS+ID+"#3333");
		
		fname = vf.createURI(NS+ADDRESS+"#firstName");
		lname = vf.createURI(NS+ADDRESS+"#lastName");
		email = vf.createURI(NS+ADDRESS+"#email");
		homeTel =vf.createURI(NS+ADDRESS+"#homeTel");
		
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
	//	testAdminCon.clear(graph1);
	//	testAdminCon.clear(graph2);
		testAdminCon.close();
		testAdminCon = null;

		testAdminRepository.shutDown();
		testAdminRepository = null;

		
		testReaderCon.clear();
		testReaderCon.close();
		testReaderCon = null;

		testReaderRepository.shutDown();
		testReaderRepository = null;
		
	/*	testWriterCon.clear();
		testWriterCon.close();
		testWriterCon = null;

		testWriterRepository.shutDown();
		testWriterRepository = null; */
        logger.info("tearDown complete.");
	}

	/**
	 * Gets an (uninitialized) instance of the repository that should be tested.
	 * 
	 * @return void
	 * @throws RepositoryConfigException 
	 * @throws RepositoryException 
	 */
	protected void  createRepository() {
		
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
        Assert.assertTrue(testAdminCon instanceof MarkLogicRepositoryConnection);
        logger.debug("testAdminCon is an instance of class MarkLogicRepositoryConnection");
        graph1 = testAdminCon.getValueFactory().createURI("http://marklogic.com/Graph1");
        graph2 = testAdminCon.getValueFactory().createURI("http://marklogic.com/Graph2");
        dirgraph = testAdminCon.getValueFactory().createURI("http://marklogic.com/dirgraph");
        
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
       
        
   /*   //Creating MLSesame Connection object Using MarkLogicRepository default constructor
        
		testWriterRepository = new MarkLogicRepository();
		testWriterRepository.setMarkLogicClient(new MarkLogicClient("localhost", restPort, "writer", "writer", "DIGEST"));
		try {
			testWriterRepository.initialize();
			Assert.assertNotNull(testWriterRepository);
			testWriterCon = testWriterRepository.getConnection();
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
		
	}
		
	@Test
	public void testPrepareBooleanQuery1() throws Exception{
		InputStream in = MarkLogicRepositoryConnectionTest.class.getResourceAsStream(TEST_DIR_PREFIX
				+ "tigers.ttl");
		testAdminCon.add(in, "", RDFFormat.TURTLE,graph1);
		in.close();
		Assert.assertEquals(107L, testAdminCon.size());
		
		String query1 = "PREFIX  bb: <http://marklogic.com/baseball/players#>"+
						" ASK FROM <http://marklogic.com/Graph1>"+
						" WHERE"+ 
						" {"+
						" ?id bb:lastname  ?name ."+
						" FILTER  EXISTS { ?id bb:country ?countryname }"+
						" }";
		
		boolean result1 = testAdminCon.prepareBooleanQuery(QueryLanguage.SPARQL, query1).evaluate();
		Assert.assertFalse(result1);	
		
		String query2 = "PREFIX  bb: <http://marklogic.com/baseball/players#>"+
				"PREFIX  r: <http://marklogic.com/baseball/rules#>"+
				" ASK  FROM <http://marklogic.com/Graph1> WHERE"+ 
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
		
		
		//Assert.assertEquals(107L, testAdminCon.size());
		reader.close();
		
		String query1 = "PREFIX  bb: <http://marklogic.com/baseball/players#>"+
				" ASK FROM <http://marklogic.com/Graph1>"+
				" WHERE"+ 
				" {"+
				 " <players#119> <players#team> <rules#Tigers>."+
				 " }";

		boolean result1 = testAdminCon.prepareBooleanQuery(QueryLanguage.SPARQL, query1,"http://marklogic.com/baseball/").evaluate();
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
		
		String currDir = System.getProperty("user.dir");
		URL url = MarkLogicRepositoryConnectionTest.class.getResource(TEST_DIR_PREFIX+"tigers.ttl");
				
		testAdminCon.add(url, "", RDFFormat.TURTLE, graph1);
		
		Assert.assertEquals(107L, testAdminCon.size());
			
		String query1 = "PREFIX  bb: <http://marklogic.com/baseball/players#>"+
				"ASK "+
				"WHERE"+ 
				"{"+
				" ?s bb:position ?o."+
				 "}";

		BooleanQuery bq = testAdminCon.prepareBooleanQuery(null, query1);
		bq.setBinding("o", vf.createLiteral("\"coach\""));
		boolean result1	= bq.evaluate();
		Assert.assertTrue(result1);	
	}
	
	@Test
	public void testPrepareBooleanQuery4() throws Exception{
		
		File file = new File(MarkLogicRepositoryConnectionTest.class.getResource(TEST_DIR_PREFIX+ "tigers.ttl").getFile());		
		testAdminCon.add(file, "", RDFFormat.TURTLE, graph1);
		logger.debug(file.getAbsolutePath());
		
		
		Assert.assertEquals(107L, testAdminCon.size());
		
		
		String query1 = "PREFIX  bb: <http://marklogic.com/baseball/players#>"+
				"ASK FROM <http://marklogic.com/Graph1>"+
				"WHERE"+ 
				"{"+
				 "<players#119> <players#lastname> \"Verlander\"<rules#Tigers>."+
				 "<players#119> <players#team> <rules#tigers>."+
				 "}";

		boolean result1 = testAdminCon.prepareBooleanQuery(query1,"http://marklogic.com/baseball/").evaluate();
		Assert.assertFalse(result1);	
	}
	

	@Test
	public void testPrepareTupleQuery1() throws Exception{
		
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
				
		try{
			Assert.assertEquals(10, testAdminCon.size(dirgraph));
		}
		catch(Exception ex){
			logger.error("Failed :", ex);
		}
			
		
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
				Assert.assertEquals(nameResult.stringValue(),"Fei");
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
		
		String [] epectedPersonresult = {"http://marklogicsparql.com/id#2222", "http://marklogicsparql.com/id#1111"};
		String [] expectedLnameresult = {"Dubinko", "Snelson"};
		int i = 0;
		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(true)));
			assertThat(result.hasNext(), is(equalTo(true)));
			while (result.hasNext()) {
				BindingSet solution = result.next();
				
				assertThat(solution.hasBinding("person"), is(equalTo(true)));
				assertThat(solution.hasBinding("lastname"), is(equalTo(true)));
				
				Value personResult = solution.getValue("person");
				Value nameResult = solution.getValue("lastname");
				
				Assert.assertEquals(nameResult.stringValue(),epectedPersonresult[i]);
				Assert.assertEquals(personResult.stringValue(),expectedLnameresult[i]);
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
		queryBuilder.append(" FROM NAMED  <Test1G> ");
		queryBuilder.append(" WHERE ");
		queryBuilder.append("  {  ");
		queryBuilder.append(" GRAPH ?g { ?id ad:lastName  ?name .} ");
		queryBuilder.append(" FILTER  EXISTS { GRAPH ?g  {?id ad:homeTel ?tel ;  ");
		queryBuilder.append("  ad:firstName ?fname.}");
		queryBuilder.append("  } ");
		queryBuilder.append(" }  ");
		queryBuilder.append(" ORDER BY $name ");
		
		TupleQuery query = testAdminCon.prepareTupleQuery(queryBuilder.toString());
		TupleQueryResult result = query.evaluate();
		
		String [] epectedPersonresult = {"http://marklogicsparql.com/id#2222", "http://marklogicsparql.com/id#1111"};
		String [] expectedLnameresult = {"Dubinko", "Snelson"};
		String [] expectedGraphresult = {"<http://marklogic.com/dirgraph>", "<http://marklogic.com/dirgraph>"};
		int i = 0;
		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(true)));
			assertThat(result.hasNext(), is(equalTo(true)));
			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertThat(solution.hasBinding("name"), is(equalTo(true)));
				assertThat(solution.hasBinding("id"), is(equalTo(true)));
				assertThat(solution.hasBinding("graph"), is(equalTo(true)));
				
				Value personResult = solution.getValue("id");
				Value nameResult = solution.getValue("name");
				Value graphResult = solution.getValue("graph");
				
				Assert.assertEquals(nameResult.stringValue(),epectedPersonresult[i]);
				Assert.assertEquals(personResult.stringValue(),expectedLnameresult[i]);
				Assert.assertEquals(personResult.stringValue(),expectedGraphresult[i]);
				i++;
			}
		}
		finally {
			result.close();
		}
	
	}
	
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
		queryBuilder.append(" SELECT ?person ?firstname ?lastname");
		queryBuilder.append(" FROM <http://marklogic.com/dirgraph>");
		queryBuilder.append(" WHERE");
		queryBuilder.append(" { ");
		queryBuilder.append("   ?person #firstName ?firstname ;");
		queryBuilder.append("           #lastName ?lastname. ");
		queryBuilder.append("   OPTIONAL {?person #homeTel ?phonenumber .} ");
		queryBuilder.append("   VALUES ?firstname { \"Micah\" \"Fei\" }");
		queryBuilder.append(" } ");
		queryBuilder.append(" ORDER BY $firstname ");
		
		TupleQuery query = testAdminCon.prepareTupleQuery(queryBuilder.toString(),"http://marklogicsparql.com/addressbook");
		TupleQueryResult result = query.evaluate();
		
		String [] epectedPersonresult = {"http://marklogicsparql.com/id#3333", "http://marklogicsparql.com/id#2222"};
		String [] expectedLnameresult = {"Ling", "Dubinko"};
		String [] expectedFnameresult = {"Fei", "Micah"};
		int i = 0;
		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(true)));
			assertThat(result.hasNext(), is(equalTo(true)));
			while (result.hasNext()) {
				BindingSet solution = result.next();
				assertThat(solution.hasBinding("person"), is(equalTo(true)));
				assertThat(solution.hasBinding("lastname"), is(equalTo(true)));
				Value personResult = solution.getValue("person");
				Value nameResult = solution.getValue("lastname");
				Assert.assertEquals(nameResult.stringValue(),epectedPersonresult[i]);
				Assert.assertEquals(personResult.stringValue(),expectedLnameresult[i]);
				Assert.assertEquals(personResult.stringValue(),expectedFnameresult[i]);
				i++;
			}
		}
		finally {
			result.close();
		}
	
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
		
		StatementList sL = new StatementList(st1);
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
		queryBuilder.append(" CONSTRUCT *");
		queryBuilder.append(" FROM {} foaf:name {name};");
		queryBuilder.append("         foaf:mbox {mbox}");
		queryBuilder.append(" USING NAMESPACE foaf = <" + FOAF_NS + ">");
		GraphQuery query = testCon.prepareGraphQuery(QueryLanguage.SERQL, queryBuilder.toString());
		query.setBinding(NAME, nameBob);
		GraphQueryResult result = query.evaluate();
		try {
			assertThat(result, is(notNullValue()));
			assertThat(result.hasNext(), is(equalTo(true)));
			while (result.hasNext()) {
				Statement st = result.next();
				URI predicate = st.getPredicate();
				assertThat(predicate, anyOf(is(equalTo(name)), is(equalTo(mbox))));
				Value object = st.getObject();
				if (name.equals(predicate)) {
					assertEquals("unexpected value for name: " + object, nameBob, object);
				}
				else {
					assertThat(predicate, is(equalTo(mbox)));
					assertEquals("unexpected value for mbox: " + object, mboxBob, object);
				}
			}
		}
		finally {
			result.close();
		}
	}

}