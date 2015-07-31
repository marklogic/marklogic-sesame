package com.marklogic.sesame.functionaltests;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.BindingSet;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.config.RepositoryConfigException;
import org.openrdf.repository.config.RepositoryFactory;
import org.openrdf.rio.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.marklogic.semantics.sesame.MarkLogicRepository;
import com.marklogic.semantics.sesame.MarkLogicRepositoryConnection;
import com.marklogic.semantics.sesame.client.MarkLogicClient;
import com.marklogic.semantics.sesame.config.MarkLogicRepositoryConfig;
import com.marklogic.semantics.sesame.config.MarkLogicRepositoryFactory;
import com.marklogic.sesame.functionaltests.util.ConnectedRESTQA;

public class MarkLogicRepositoryConnectionTest  extends  ConnectedRESTQA{

	private static final String TEST_DIR_PREFIX = "/testdata/";
	private static String dbName = "MLSesame";
	private static String [] fNames = {"MLSesame-1"};
	private static String restServer = "REST-MLSesame-API-Server";
	private static int restPort = 8011;
	private static int uberPort = 8000;

	
	protected static Repository testAdminRepository;
	protected static Repository testReaderRepository;
	protected static MarkLogicRepository testWriterRepository;
	protected static RepositoryConnection testAdminCon;
	protected static RepositoryConnection testReaderCon;
	protected static RepositoryConnection testWriterCon;
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
		logger.debug("tearing down...");
		
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
			testAdminRepository = factory.getRepository(adminconfig);
		} catch (RepositoryConfigException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        try {
			testAdminRepository.initialize();
			testAdminCon = testAdminRepository.getConnection();
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
			testReaderCon = testReaderRepository.getConnection();
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
		testAdminCon.add(in, "", RDFFormat.TURTLE, graph1);
		in.close();
		Assert.assertEquals(107L, testAdminCon.size());
		
		String query1 = "PREFIX  bb: <http://marklogic.com/baseball/players#>"+
						"ASK FROM <http://marklogic.com/Graph1>"+
						"WHERE"+ 
						"{"+
						 "?id bb:lastname  ?name ."+
						    "FILTER  EXISTS { ?id bb:country ?countryname }"+
						"}";
		
		boolean result1 = testAdminCon.prepareBooleanQuery(QueryLanguage.SPARQL, query1).evaluate();
		Assert.assertFalse(result1);	
		
		String query2 = "PREFIX  bb: <http://marklogic.com/baseball/players#>"+
				"PREFIX  r: <http://marklogic.com/baseball/rules#>"+
				"ASK  FROM <http://marklogic.com/Graph1> WHERE"+ 
				"{"+
				 "?id bb:team r:Tigers."+
				    "?id bb:position \"pitcher\"."+
				"}";

		boolean result2 = testAdminCon.prepareBooleanQuery(QueryLanguage.SPARQL, query2).evaluate();
		Assert.assertTrue(result2);	
	}
	
	@Test
	public void testPrepareBooleanQuery2() throws Exception{
		
		InputStream in = MarkLogicRepositoryConnectionTest.class.getResourceAsStream(TEST_DIR_PREFIX
				+ "tigers.ttl");		
		Reader reader = new InputStreamReader(in);
		testAdminCon.add(reader, "http://marklogic.com/baseball/", RDFFormat.TURTLE, graph1);
		
		
		Assert.assertEquals(107L, testAdminCon.size());
		reader.close();
		
		String query1 = "PREFIX  bb: <http://marklogic.com/baseball/players#>"+
				"ASK FROM <http://marklogic.com/Graph1>"+
				"WHERE"+ 
				"{"+
				 "<players#119> <players#team> <rules#Tigers>."+
				 "}";

		boolean result1 = testAdminCon.prepareBooleanQuery(QueryLanguage.SPARQL, query1,"http://marklogic.com/baseball/").evaluate();
		Assert.assertTrue(result1);	
		
		String query2 = "PREFIX  bb: <http://marklogic.com/baseball/players#>"+
				"PREFIX  r: <http://marklogic.com/baseball/rules#>"+
				"ASK  FROM <http://marklogic.com/Graph1> WHERE"+ 
				"{"+
				 "?id bb:team r:Tigers."+
				    "?id bb:position \"pitcher\"."+
				"}";

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
		catch(Exception e){
			logger.error("Failed :", e);
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

}