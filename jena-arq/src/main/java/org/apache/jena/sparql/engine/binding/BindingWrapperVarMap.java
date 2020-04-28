package org.apache.jena.sparql.engine.binding;

import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.util.FmtUtils;

public class BindingWrapperVarMap implements Binding {
	
	protected Binding binding ;
	protected Map<Var, Var> mapping;

	public BindingWrapperVarMap(Binding binding, Map<String, String> mapping) {
		this.binding = binding;
		this.mapping = mapping.entrySet().stream().collect(Collectors.toMap(e-> Var.alloc(e.getKey()), e -> Var.alloc(e.getValue())));
	}

	@Override
	public Iterator<Var> vars() {
		return mapping.keySet().iterator();
	}

	@Override
	public boolean contains(Var var) {
		return binding.contains(mapping.get(var));
	}

	@Override
	public Node get(Var var) {
		return binding.get(mapping.get(var));
	}

	@Override
	public int size() {
		return binding.size();
	}

	@Override
	public boolean isEmpty() {
		return binding.isEmpty();
	}
	
	@Override
	public String toString() {
		StringBuffer sbuff = new StringBuffer();
		String sep = "";
        for ( Iterator<Var> iter = vars() ; iter.hasNext() ; ) {
            Object obj = iter.next();
            Var var = (Var)obj;

            sbuff.append(sep);
            sep = " ";
            Node node = get(var);
            String tmp = FmtUtils.stringForObject(node);
            sbuff.append("( ?" + var.getVarName() + " = " + tmp + " )");
        }
        return sbuff.toString();
	}

}
