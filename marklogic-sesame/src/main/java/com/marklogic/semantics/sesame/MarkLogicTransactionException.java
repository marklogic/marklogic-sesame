package com.marklogic.semantics.sesame;

/**
 * specific exception for throwing MarkLogic transaction exceptions
 */
@SuppressWarnings("serial")
public class MarkLogicTransactionException extends MarkLogicSesameException {
    public MarkLogicTransactionException(String message) {
        super(message);
    }

}
