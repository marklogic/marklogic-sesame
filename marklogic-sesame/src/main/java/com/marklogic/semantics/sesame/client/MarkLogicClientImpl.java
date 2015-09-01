/*
 * Copyright 2015 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * A library that enables access to a MarkLogic-backed triple-store via the
 * Sesame API.
 */
package com.marklogic.semantics.sesame.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.FailedRequestException;
import com.marklogic.client.Transaction;
import com.marklogic.client.impl.SPARQLBindingsImpl;
import com.marklogic.client.io.FileHandle;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.client.query.QueryDefinition;
import com.marklogic.client.semantics.*;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.Binding;
import org.openrdf.repository.sparql.query.SPARQLQueryBindingSet;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Locale;

/**
 * internal class for interacting with java api client
 *
 * @author James Fuller
 */
public class MarkLogicClientImpl {

    protected final Logger logger = LoggerFactory.getLogger(MarkLogicClientImpl.class);

    private static final String DEFAULT_GRAPH_URI = "http://marklogic.com/semantics#default-graph";

    private String host;

    private int port;

    private String user;

    private String password;

    private String auth;

    protected DatabaseClientFactory.Authentication authType = DatabaseClientFactory.Authentication.valueOf(
            "DIGEST"
    );

    private SPARQLRuleset rulesets;
    private QueryDefinition constrainingQueryDef;
    private GraphPermissions graphPerms;

    private SPARQLQueryManager sparqlManager;
    private GraphManager graphManager;

    private DatabaseClient databaseClient;

    // constructor
    public MarkLogicClientImpl(String host, int port, String user, String password, String auth) {
        setDatabaseClient(DatabaseClientFactory.newClient(host, port, user, password, DatabaseClientFactory.Authentication.valueOf(auth)));
    }

    public MarkLogicClientImpl(Object databaseClient) {
        if (databaseClient instanceof DatabaseClient) {
            setDatabaseClient((DatabaseClient) databaseClient);
        }
    }

    private void setDatabaseClient(DatabaseClient databaseClient) {
        this.databaseClient = databaseClient;
    }

    // host
    public String getHost() {
        return this.host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    // port
    public int getPort() {
        return this.port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    // user
    public String getUser() {
        return this.user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    // password
    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    // auth
    public String getAuth() {
        return auth;
    }

    public void setAuth(String auth) {
        this.auth = auth;
        this.authType = DatabaseClientFactory.Authentication.valueOf(
                auth
        );
    }

    // auth type
    public void setAuthType(DatabaseClientFactory.Authentication authType) {
        this.authType = authType;
    }

    public DatabaseClientFactory.Authentication getAuthType() {
        return authType;
    }

    //
    public DatabaseClient getDatabaseClient() {
        return this.databaseClient;
    }

    // performSPARQLQuery
    public InputStream performSPARQLQuery(String queryString, SPARQLQueryBindingSet bindings, long start, long pageLength, Transaction tx, boolean includeInferred, String baseURI) throws JsonProcessingException {
        return performSPARQLQuery(queryString, bindings, new InputStreamHandle(), start, pageLength, tx, includeInferred, baseURI);
    }

    public InputStream performSPARQLQuery(String queryString, SPARQLQueryBindingSet bindings, InputStreamHandle handle, long start, long pageLength, Transaction tx, boolean includeInferred, String baseURI) throws JsonProcessingException {
        sparqlManager = getDatabaseClient().newSPARQLQueryManager();
        StringBuilder sb = new StringBuilder();
        if (notNull(baseURI) && baseURI != "") sb.append("BASE <" + baseURI + ">\n");
        sb.append(queryString);
        SPARQLQueryDefinition qdef = sparqlManager.newQueryDefinition(sb.toString());
        if (rulesets instanceof SPARQLRuleset) {
            qdef.setRulesets(rulesets);
        }

        if (getConstrainingQueryDefinition() instanceof QueryDefinition) {
            qdef.setConstrainingQueryDefinition(getConstrainingQueryDefinition());
        }

        qdef.setIncludeDefaultRulesets(includeInferred);
        qdef.setBindings(getSPARQLBindings(bindings));
        sparqlManager.setPageLength(pageLength);
        sparqlManager.executeSelect(qdef, handle, start, tx);
        return handle.get();
    }

    // performGraphQuery
    public InputStream performGraphQuery(String queryString, SPARQLQueryBindingSet bindings, Transaction tx, boolean includeInferred, String baseURI) throws JsonProcessingException {
        return performGraphQuery(queryString, bindings, new InputStreamHandle(), tx, includeInferred, baseURI);
    }

    public InputStream performGraphQuery(String queryString, SPARQLQueryBindingSet bindings, InputStreamHandle handle, Transaction tx, boolean includeInferred, String baseURI) throws JsonProcessingException {
        sparqlManager = getDatabaseClient().newSPARQLQueryManager();
        StringBuilder sb = new StringBuilder();
        if (notNull(baseURI) && baseURI != "") sb.append("BASE <" + baseURI + ">\n");
        sb.append(queryString);
        SPARQLQueryDefinition qdef = sparqlManager.newQueryDefinition(sb.toString());
        if (rulesets instanceof SPARQLRuleset) {
            qdef.setRulesets(rulesets);
        }

        if (getConstrainingQueryDefinition() instanceof QueryDefinition) {
            qdef.setConstrainingQueryDefinition(getConstrainingQueryDefinition());
        }

        qdef.setIncludeDefaultRulesets(includeInferred);
        qdef.setBindings(getSPARQLBindings(bindings));
        sparqlManager.executeDescribe(qdef, handle, tx);
        return handle.get();
    }

    // performBooleanQuery
    public boolean performBooleanQuery(String queryString, SPARQLQueryBindingSet bindings, Transaction tx, boolean includeInferred, String baseURI) {
        sparqlManager = getDatabaseClient().newSPARQLQueryManager();
        StringBuilder sb = new StringBuilder();
        if (notNull(baseURI) && baseURI != "") sb.append("BASE <" + baseURI + ">\n");
        sb.append(queryString);
        SPARQLQueryDefinition qdef = sparqlManager.newQueryDefinition(sb.toString());
        qdef.setIncludeDefaultRulesets(includeInferred);
        if (rulesets instanceof SPARQLRuleset) {
            qdef.setRulesets(rulesets);
        }

        if (getConstrainingQueryDefinition() instanceof QueryDefinition) {
            qdef.setConstrainingQueryDefinition(getConstrainingQueryDefinition());
        }

        qdef.setBindings(getSPARQLBindings(bindings));
        return sparqlManager.executeAsk(qdef, tx);
    }

    // performUpdateQuery
    public void performUpdateQuery(String queryString, SPARQLQueryBindingSet bindings, Transaction tx, boolean includeInferred, String baseURI) {
        sparqlManager = getDatabaseClient().newSPARQLQueryManager();
        StringBuilder sb = new StringBuilder();
        if (notNull(baseURI) && baseURI != "") sb.append("BASE <" + baseURI + ">\n");
        sb.append(queryString);
        SPARQLQueryDefinition qdef = sparqlManager.newQueryDefinition(sb.toString());
        if (rulesets instanceof SPARQLRuleset) {
            qdef.setRulesets(rulesets);
        }

        if (getConstrainingQueryDefinition() instanceof QueryDefinition) {
            qdef.setConstrainingQueryDefinition(getConstrainingQueryDefinition());
        }

        qdef.setIncludeDefaultRulesets(includeInferred);
        qdef.setBindings(getSPARQLBindings(bindings));
        sparqlManager.executeUpdate(qdef, tx);
    }

    // performAdd
    // TBD- need to wire in baseURI when method is exposed to java api client
    public void performAdd(File file, String baseURI, RDFFormat dataFormat, Transaction tx, Resource... contexts) throws RDFParseException {
        try {
            graphManager = getDatabaseClient().newGraphManager();

            graphManager.setDefaultMimetype(dataFormat.getDefaultMIMEType());
            if (dataFormat.equals(RDFFormat.NQUADS) || dataFormat.equals(RDFFormat.TRIG)) {
                //TBD- tx ?
                graphManager.mergeGraphs(new FileHandle(file));
            } else {
                //TBD- must be more efficient
                if (notNull(contexts) && contexts.length>0) {
                    for (int i = 0; i < contexts.length; i++) {
                        if(notNull(contexts[i])){
                            graphManager.merge(contexts[i].toString(), new FileHandle(file), tx);
                        }else{
                            graphManager.merge(DEFAULT_GRAPH_URI, new FileHandle(file), tx);
                        }
                    }
                } else {
                    graphManager.merge(DEFAULT_GRAPH_URI, new FileHandle(file), tx);
                }
            }
        } catch (FailedRequestException e) {
            throw new RDFParseException("Request to MarkLogic server failed, check file and format.");
        }
    }

    // TBD- need to wire in baseURI when method is exposed to java api client
    public void performAdd(InputStream in, String baseURI, RDFFormat dataFormat, Transaction tx, Resource... contexts) {
        graphManager = getDatabaseClient().newGraphManager();
        graphManager.setDefaultMimetype(dataFormat.getDefaultMIMEType());
        if (dataFormat.equals(RDFFormat.NQUADS) || dataFormat.equals(RDFFormat.TRIG)) {
            //TBD- tx ?
            graphManager.mergeGraphs(new InputStreamHandle(in));
        } else {
            //TBD- must be more efficient
            if (notNull(contexts) && contexts.length>0) {
                for (int i = 0; i < contexts.length; i++) {
                    if(notNull(contexts[i])){
                        graphManager.merge(contexts[i].toString(),  new InputStreamHandle(in), tx);
                    }else{
                        graphManager.merge(DEFAULT_GRAPH_URI,  new InputStreamHandle(in), tx);
                    }
                }
            } else {
                graphManager.merge(DEFAULT_GRAPH_URI, new InputStreamHandle(in), tx);
            }
        }
    }

    public void performAdd(String baseURI, Resource subject, URI predicate, Value object, Transaction tx, Resource... contexts) {
        sparqlManager = getDatabaseClient().newSPARQLQueryManager();
        StringBuilder sb = new StringBuilder();
        if (notNull(baseURI) && baseURI != "") sb.append("BASE <" + baseURI + ">\n");
        if(notNull(contexts) && contexts.length>0) {
            sb.append("INSERT DATA { ");
            for (int i = 0; i < contexts.length; i++) {
                if (notNull(contexts[i])) {
                    sb.append("GRAPH <" + contexts[i].stringValue() + "> { ?s ?p ?o .} ");
                } else {
                    sb.append("GRAPH <" + DEFAULT_GRAPH_URI + "> { ?s ?p ?o .} ");
                }
            }
            sb.append("}");
        }else {
            sb.append("INSERT DATA { GRAPH <" + DEFAULT_GRAPH_URI + "> {?s ?p ?o .}}");
        }
        SPARQLQueryDefinition qdef = sparqlManager.newQueryDefinition(sb.toString());
        if(notNull(subject)) qdef.withBinding("s", subject.stringValue());
        if(notNull(predicate)) qdef.withBinding("p", predicate.stringValue());
        if(notNull(object)) bindObject(qdef, "o", object);
        sparqlManager.executeUpdate(qdef, tx);
    }

    // performRemove
    public void performRemove(String baseURI, Resource subject, URI predicate, Value object, Transaction tx, Resource... contexts) {
        StringBuilder sb = new StringBuilder();
        if (notNull(baseURI) && baseURI != "") sb.append("BASE <" + baseURI + ">\n");
        if(notNull(contexts) && contexts.length>0) {
            sb.append("DELETE WHERE { ");
            for (int i = 0; i < contexts.length; i++) {
                if (notNull(contexts[i])) {
                    sb.append("GRAPH <" + contexts[i].stringValue() + "> { ?s ?p ?o .} ");
                } else {
                    sb.append("GRAPH <" + DEFAULT_GRAPH_URI + "> { ?s ?p ?o .} ");
                }
            }
            sb.append("}");
        }else{
            sb.append("DELETE WHERE { GRAPH ?ctx{ ?s ?p ?o .}}");
        }
        SPARQLQueryDefinition qdef = sparqlManager.newQueryDefinition(sb.toString());
        if(notNull(subject)) qdef.withBinding("s", subject.stringValue());
        if(notNull(predicate)) qdef.withBinding("p", predicate.stringValue());
        if(notNull(object)) bindObject(qdef, "o", object);
        sparqlManager.clearPageLength();
        sparqlManager.executeUpdate(qdef, tx);
    }

    // performClear
    public void performClear(Transaction tx, Resource... contexts) {
        graphManager = getDatabaseClient().newGraphManager();
        if(notNull(contexts)) {
            for (int i = 0; i < contexts.length; i++) {
                if (notNull(contexts[i])) {
                    graphManager.delete(contexts[i].stringValue(), tx);
                } else {
                    graphManager.delete(DEFAULT_GRAPH_URI, tx);
                }
            }
        }else{
            graphManager.delete(DEFAULT_GRAPH_URI, tx);
        }
    }

    public void performClearAll(Transaction tx) {
        graphManager = getDatabaseClient().newGraphManager();
        graphManager.deleteGraphs();
    }

    // rulesets
    public SPARQLRuleset getRulesets() {
        return this.rulesets;
    }

    public void setRulesets(Object rulesets) {
        this.rulesets = (SPARQLRuleset) rulesets;
    }

    // graph perms
    public void setGraphPerms(Object graphPerms) {
        this.graphPerms = (GraphPermissions) graphPerms;
    }

    public GraphPermissions getGraphPerms() {
        return this.graphPerms;
    }

    // constraining query
    public void setConstrainingQueryDefinition(Object constrainingQueryDefinition) {
        this.constrainingQueryDef = (QueryDefinition) constrainingQueryDefinition;
    }

    public QueryDefinition getConstrainingQueryDefinition() {
        return this.constrainingQueryDef;
    }

    // getSPARQLBindings
    protected SPARQLBindings getSPARQLBindings(SPARQLQueryBindingSet bindings) {
        SPARQLBindings sps = new SPARQLBindingsImpl();
        for (Binding binding : bindings) {
            sps.bind(binding.getName(), binding.getValue().stringValue());
            logger.debug("binding:" + binding.getName() + "=" + binding.getValue());
        }
        return sps;
    }

    private static SPARQLQueryDefinition bindObject(SPARQLQueryDefinition qdef, String variableName, Value object){
        SPARQLBindings bindings = qdef.getBindings();
        if(object != null){
            if (object instanceof URI) {
                bindings.bind(variableName, object.stringValue());
            } else if (object instanceof Literal) {
                Literal lit = (Literal) object;
                if (((Literal) object).getDatatype() != null) {
                    try {
                        String xsdType = lit.getDatatype().toString();
                        String fragment = new java.net.URI(xsdType).getFragment();
                        bindings.bind(variableName, lit.getLabel(), fragment);
                    } catch (URISyntaxException e) {
                        //throw new MarkLogicSesameException("Problem with object.");
                    }
                } else if (!lit.getLanguage().equals("")) {
                    String languageTag = lit.getLanguage();
                    bindings.bind(variableName, lit.getLabel(), Locale.forLanguageTag(languageTag));
                } else {
                    bindings.bind(variableName, lit.getLabel(), "string");
                }
            }
            qdef.setBindings(bindings);
        }
        return qdef;
    }

    private Boolean notNull(Object item) {
        if (item!=null)
            return true;
        else
            return false;
    }
}