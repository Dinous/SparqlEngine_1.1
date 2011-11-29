/**.
 * 
 */
package org.liris.sparql.functions;

import java.util.List;

import com.hp.hpl.jena.sparql.ARQInternalErrorException;
import com.hp.hpl.jena.sparql.expr.ExprEvalException;
import com.hp.hpl.jena.sparql.expr.ExprList;
import com.hp.hpl.jena.sparql.expr.NodeValue;
import com.hp.hpl.jena.sparql.expr.nodevalue.XSDFuncOp;
import com.hp.hpl.jena.sparql.function.FunctionBase;
import com.hp.hpl.jena.sparql.util.Utils;

/**.
 * @author Dino
 * @30 sept. 2011
 */
public class Max extends FunctionBase {

    @Override
    public void checkBuild(String uri, ExprList args)
    { 
    }

    
    @Override
    public final NodeValue exec(List<NodeValue> args)
    {
        if ( args == null )
            // The contract on the function interface is that this should not happen.
            throw new ARQInternalErrorException(Utils.className(this)+": Null args list") ;
        
        if ( args.size() < 2 )
            throw new ExprEvalException(Utils.className(this)+": Wrong number of arguments: Wanted at least 2, got "+args.size()) ;
        
        NodeValue nv = args.get(0);
	for (NodeValue nodeValue : args) {
	    nv = XSDFuncOp.max(nv, nodeValue);
	}
	return nv;
    }

}
