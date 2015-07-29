package com.marklogic.sesame.functionaltests;

import java.io.InputStream;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
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

	private static final String TEST_DIR_PREFIX = "/testdata";
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
	
	@BeforeClass
	public static void initialSetup() throws Exception {
		
		setupJavaRESTServer(dbName, fNames[0], restServer, restPort);
		setupAppServicesConstraint(dbName);
		enableCollectionLexicon(dbName);
		enableTripleIndex(dbName);
		
		createUserRolesWithPrevilages("test-eval", "xdbc:eval", "xdbc:eval-in", "xdmp:eval-in", "any-uri", "xdbc:invoke");
		createRESTUser("reader", "reader", "test-eval", "rest-reader");
		createRESTUser("writer", "writer", "test-eval", "rest-writer");
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
		
	}

	@After
	public void tearDown()
		throws Exception
	{
		logger.debug("tearing down...");
		
		testAdminCon.clear();
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
		// add file default-graph.ttl to repository, no context
		InputStream in = MarkLogicRepositoryConnectionTest.class.getResourceAsStream(TEST_DIR_PREFIX
				+ "directory.ttl");
		testAdminCon.add(in, "", RDFFormat.TURTLE, graph1);
		in.close();
		Assert.assertEquals(20L, testAdminCon.size());
		
		

	
		
	}
}