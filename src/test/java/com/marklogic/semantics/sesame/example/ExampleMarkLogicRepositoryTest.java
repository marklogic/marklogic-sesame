package com.marklogic.semantics.sesame.example;

import com.marklogic.semantics.sesame.MarkLogicRepository;
import org.junit.Ignore;
import org.openrdf.model.ValueFactory;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFHandlerException;

/**
 * Created by jfuller on 6/25/15.
 */
public class ExampleMarkLogicRepositoryTest {

    @Ignore
    public void testHello() throws RepositoryException, RDFHandlerException {

        Repository rep = new MarkLogicRepository("test");
        rep.initialize();

        ValueFactory f = rep.getValueFactory();
        String namespace = "http://example.org/";

//        URI john = f.createURI(namespace, "john");
//
//        RepositoryConnection conn = rep.getConnection();
//        try {
//            conn.add(john, RDF.TYPE, FOAF.PERSON);
//            conn.add(john, RDFS.LABEL, f.createLiteral("John", XMLSchema.STRING));
//
//            RepositoryResult<Statement> statements =
//                    conn.getStatements(null, null, null, true);
//
//            Model model = Iterations.addAll(statements, new LinkedHashModel());
//            model.setNamespace("rdf", RDF.NAMESPACE);
//            model.setNamespace("rdfs", RDFS.NAMESPACE);
//            model.setNamespace("xsd", XMLSchema.NAMESPACE);
//            model.setNamespace("foaf", FOAF.NAMESPACE);
//            model.setNamespace("ex", namespace);
//
//            Rio.write(model, System.out, RDFFormat.TURTLE);
//        }
//        finally {
//            conn.close();
//        }
    }

}