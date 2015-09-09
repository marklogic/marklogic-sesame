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
package com.marklogic.semantics.sesame.query;

import com.marklogic.client.query.QueryDefinition;
import com.marklogic.client.semantics.GraphPermissions;
import com.marklogic.client.semantics.SPARQLRuleset;

/**
 * interface defining MarkLogic specific query features
 *
 * @author James Fuller
 */
interface MarkLogicQueryDependent {

    /**
     * gets query inference ruleset
     * @return java api client Ruleset
     */
    SPARQLRuleset[] getRulesets();

    /**
     * sets query inference ruleset to be used when query executes
     * @param ruleset
     */
    void setRulesets(SPARQLRuleset ... ruleset);

    /**
     * gets base uri to be used by query
     * @return string
     */
    String getBaseURI();

    /**
     * sets base uri to be used by query
     * @param baseURI
     */
    void setBaseURI(String baseURI);

    /**
     * gets constraining query to be used by query
     * @return
     */
    QueryDefinition getConstrainingQueryDefinition();

    /**
     * sets constraining query to be used by query
     *
     * @param constrainingQueryDefinition
     */
    void setConstrainingQueryDefinition(QueryDefinition constrainingQueryDefinition);

    /**
     *  gets graph permissions to be used by query
     */
    GraphPermissions getGraphPerms();

    /**
     * sets graph permissions to be used by query
     * @param graphPerms
     */
    void setGraphPerms(GraphPermissions graphPerms);

}
