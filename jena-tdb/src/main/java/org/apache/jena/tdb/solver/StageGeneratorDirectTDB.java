/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jena.tdb.solver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.jena.atlas.lib.tuple.Tuple ;
import org.apache.jena.graph.Graph ;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.BasicPattern ;
import org.apache.jena.sparql.engine.ExecutionContext ;
import org.apache.jena.sparql.engine.QueryIterator ;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingWrapperVarMap;
import org.apache.jena.sparql.engine.iterator.BufferedQueryIteratorFactory;
import org.apache.jena.sparql.engine.iterator.QueryIterPlainWrapper;
import org.apache.jena.sparql.engine.iterator.QueryIterRoot;
import org.apache.jena.sparql.engine.join.Join;
import org.apache.jena.sparql.engine.main.StageGenerator ;
import org.apache.jena.sparql.util.Symbol;
import org.apache.jena.tdb.store.GraphTDB ;
import org.apache.jena.tdb.store.NodeId;

/** Execute TDB requests directly -- no reordering
 *  Using OpExecutor is preferred.
 */ 
public class StageGeneratorDirectTDB implements StageGenerator
{
    // Using OpExecutor is preferred.
    StageGenerator above = null ;
    
    public StageGeneratorDirectTDB(StageGenerator original)
    {
        above = original ;
    }
    
    @Override
    public QueryIterator execute(BasicPattern pattern, QueryIterator input, ExecutionContext execCxt)
    {
        // --- In case this isn't for TDB
        Graph g = execCxt.getActiveGraph() ;
        
        if ( ! ( g instanceof GraphTDB ) )
            // Not us - bounce up the StageGenerator chain
            return above.execute(pattern, input, execCxt) ;
        GraphTDB graph = (GraphTDB)g ;
        Predicate<Tuple<NodeId>> filter = QC2.getFilter(execCxt.getContext()) ;
        return SolverLib.execute(graph, pattern, input, filter, execCxt) ;
    }

	@Override
	public QueryIterator executeWithRP(BasicPattern pattern, QueryIterator input, ExecutionContext execCxt) {
		BasicPattern other = (BasicPattern)execCxt.getContext().get(Symbol.create("RIGHT_PATTERN"));
		if (other == null || other.size()==0) {
			throw new UnsupportedOperationException("RHS has an empty BGP");
		}
		
		Graph g = execCxt.getActiveGraph() ;        
        if ( ! ( g instanceof GraphTDB ) )
            return above.execute(pattern, input, execCxt) ;
        GraphTDB graph = (GraphTDB)g ;
        Predicate<Tuple<NodeId>> filter = QC2.getFilter(execCxt.getContext()) ;
		Map<String, String> varmap = new HashMap<String, String>();
		BasicPattern overlap = overlappingPattern(pattern, other, varmap);
		QueryIterator partial = SolverLib.execute(graph, overlap, input, filter, execCxt);
		BufferedQueryIteratorFactory partialFactory = new BufferedQueryIteratorFactory(partial);
		execCxt.getContext().put(Symbol.create("OVERLAP_ITERATOR"), partialFactory.createBufferedQueryIterator());
		execCxt.getContext().put(Symbol.create("OVERLAP_PATTERN"), overlap);
		execCxt.getContext().put(Symbol.create("VARMAP"), varmap);
		BasicPattern diff = patternDiff(pattern, overlap);
		if (diff.size()==0) {
			return partialFactory.createBufferedQueryIterator();
		}
		QueryIterator rest = SolverLib.execute(graph, diff, QueryIterRoot.create(execCxt), filter, execCxt);
		return Join.join(partialFactory.createBufferedQueryIterator(), rest, execCxt) ;
	}
	
	@Override
	public QueryIterator executeWithOverlap(BasicPattern pattern, QueryIterator input, ExecutionContext execCxt) {
		Graph g = execCxt.getActiveGraph() ;        
        if ( ! ( g instanceof GraphTDB ) )
            return above.execute(pattern, input, execCxt) ;
        GraphTDB graph = (GraphTDB)g ;
        Predicate<Tuple<NodeId>> filter = QC2.getFilter(execCxt.getContext()) ;
		QueryIterator partial = wrap(execCxt.getContext().get(Symbol.create("OVERLAP_ITERATOR")), execCxt.getContext().get(Symbol.create("VARMAP")));
		BasicPattern diff = patternDiff(pattern, execCxt.getContext().get(Symbol.create("OVERLAP_PATTERN")));
		if (diff.size()==0) {
			return partial;
		}
		return Join.join(partial, SolverLib.execute(graph, diff, QueryIterRoot.create(execCxt), filter, execCxt), execCxt);
	}

	private QueryIterator wrap(QueryIterator qIter, Map<String, String> varmap) {
		List<Binding> res = new ArrayList<Binding>();
		while(qIter.hasNext()) {
			Binding b = new BindingWrapperVarMap(qIter.nextBinding(), varmap);
			res.add(b);
		}
		return new QueryIterPlainWrapper(res.iterator());
	}

	private BasicPattern patternDiff(BasicPattern p, BasicPattern q) {
		BasicPattern diff = new BasicPattern();
		for(Triple left : p) {
			boolean contained = false;
			for( Triple right : q) {
				Node sl = left.getSubject();
				Node sr = right.getSubject();
				Node pl = left.getPredicate();
				Node pr = right.getPredicate();
				Node ol = left.getObject();
				Node or = right.getObject();
				if(sl.isVariable() || sr.isVariable() || sl.sameValueAs(sr)) {
					if(pl.isVariable() || pr.isVariable() || pl.sameValueAs(pr)) {
						if(ol.isVariable() || or.isVariable() || ol.sameValueAs(or)) {
							contained = true;
							break;
						}
					}
				}
			}
			if (!contained) {
				diff.add(left);	
			}
		}
		return diff;
	}

	private BasicPattern overlappingPattern(BasicPattern pattern, BasicPattern other, Map<String, String> varmap) {
		BasicPattern overlap = new BasicPattern();
		for(int i=0; i<Math.min(pattern.size(), other.size()) ;i++) {
			Triple left = pattern.get(i);
			Triple right = other.get(i);
			Node sl = left.getSubject();
			Node sr = right.getSubject();
			if(!sl.isVariable() && !sr.isVariable() && !sl.sameValueAs(sr)) break;
			Node pl = left.getPredicate();
			Node pr = right.getPredicate();
			if(!pl.isVariable() && !pr.isVariable() && !pl.sameValueAs(pr)) break;
			Node ol = left.getObject();
			Node or = right.getObject();
			if(!ol.isVariable() && !or.isVariable() && !ol.sameValueAs(or)) break;
			overlap.add(left);
			for(int j=0; j<left.getVars().size(); j++) {
				varmap.put(right.getVars().get(j), left.getVars().get(j));
			}
		}
		return overlap;
	}
    
}
