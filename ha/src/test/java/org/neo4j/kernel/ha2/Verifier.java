package org.neo4j.kernel.ha2;

public interface Verifier<STATE>
{
    /**
     * Throws an {@link IllegalStateException} if it doesn't verify correctly.
     * @param state the state to verify.
     */
    void verify( STATE state );
}
