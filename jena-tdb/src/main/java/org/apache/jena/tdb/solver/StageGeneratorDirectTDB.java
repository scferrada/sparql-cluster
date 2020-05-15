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

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.atlas.lib.PairOfSameType;
import org.apache.jena.atlas.lib.tuple.Tuple;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.iterator.BufferedQueryIteratorFactory;
import org.apache.jena.sparql.engine.iterator.QueryIter;
import org.apache.jena.sparql.engine.iterator.QueryIterRoot;
import org.apache.jena.sparql.engine.join.Join;
import org.apache.jena.sparql.engine.main.StageGenerator;
import org.apache.jena.sparql.util.Symbol;
import org.apache.jena.tdb.store.GraphTDB;
import org.apache.jena.tdb.store.NodeId;

/**
 * Execute TDB requests directly -- no reordering Using OpExecutor is preferred.
 */
public class StageGeneratorDirectTDB implements StageGenerator {
	// Using OpExecutor is preferred.
	StageGenerator above = null;

	public StageGeneratorDirectTDB(StageGenerator original) {
		above = original;
	}

	@Override
	public QueryIterator execute(BasicPattern pattern, QueryIterator input, ExecutionContext execCxt) {
		// --- In case this isn't for TDB
		Graph g = execCxt.getActiveGraph();

		if (!(g instanceof GraphTDB))
			// Not us - bounce up the StageGenerator chain
			return above.execute(pattern, input, execCxt);
		GraphTDB graph = (GraphTDB) g;
		Predicate<Tuple<NodeId>> filter = QC2.getFilter(execCxt.getContext());
		return SolverLib.execute(graph, pattern, input, filter, execCxt);
	}

	@Override
	public QueryIterator executeWithRP(BasicPattern pattern, QueryIterator input, ExecutionContext execCxt) {
		BasicPattern other = (BasicPattern) execCxt.getContext().get(Symbol.create("RIGHT_PATTERN"));
		if (other == null || other.size() == 0) {
			throw new UnsupportedOperationException("RHS has an empty BGP");
		}

		Graph g = execCxt.getActiveGraph();
		if (!(g instanceof GraphTDB))
			return above.execute(pattern, input, execCxt);
		GraphTDB graph = (GraphTDB) g;
		Predicate<Tuple<NodeId>> filter = QC2.getFilter(execCxt.getContext());
		Map<Var, Var> varmap = new HashMap<Var, Var>();
		Pair<BasicPattern, PairOfSameType<BasicPattern>> result = overlappingPattern(pattern, other, varmap);
		BasicPattern overlap = result.getLeft();
		QueryIterator partial = SolverLib.execute(graph, overlap, input, filter, execCxt);
		BufferedQueryIteratorFactory partialFactory = new BufferedQueryIteratorFactory(partial);
		execCxt.getContext().put(Symbol.create("OVERLAP_ITERATOR"), partialFactory.createBufferedQueryIterator());
		execCxt.getContext().put(Symbol.create("OVERLAP_PATTERN"), overlap);
		execCxt.getContext().put(Symbol.create("VARMAP"), varmap);
		BasicPattern diff = result.getRight().getLeft();
		execCxt.getContext().put(Symbol.create("OVERLAP_DIFF"), result.getRight().getRight());
		if (diff.size() == 0) {
			return partialFactory.createBufferedQueryIterator();
		}
		QueryIterator rest = SolverLib.execute(graph, diff, QueryIterRoot.create(execCxt), filter, execCxt);
		return Join.join(partialFactory.createBufferedQueryIterator(), rest, execCxt);
	}

	@Override
	public QueryIterator executeWithOverlap(BasicPattern pattern, QueryIterator input, ExecutionContext execCxt) {
		Graph g = execCxt.getActiveGraph();
		if (!(g instanceof GraphTDB))
			return above.execute(pattern, input, execCxt);
		GraphTDB graph = (GraphTDB) g;
		Predicate<Tuple<NodeId>> filter = QC2.getFilter(execCxt.getContext());
		Map<Var, Var> varmap = execCxt.getContext().get(Symbol.create("VARMAP"));
		QueryIterator partial = QueryIter.map(execCxt.getContext().get(Symbol.create("OVERLAP_ITERATOR")), varmap );
		BasicPattern diff = execCxt.getContext().get(Symbol.create("OVERLAP_DIFF"));
		if (diff.size() == 0) {
			return partial;
		}
		return Join.join(partial, SolverLib.execute(graph, diff, QueryIterRoot.create(execCxt), filter, execCxt),
				execCxt);
	}

	private Pair<BasicPattern, PairOfSameType<BasicPattern>> overlappingPattern(BasicPattern pattern, BasicPattern other, Map<Var, Var> varmap) {
		BasicPattern overlap = new BasicPattern();
		BasicPattern diffLeft = new BasicPattern();
		BasicPattern diffRight = new BasicPattern();
		for (int i = 0; i < Math.min(pattern.size(), other.size()); i++) {
			Triple left = pattern.get(i);
			Triple right = other.get(i);
			if (left.matches(right)) {
				overlap.add(left);
			} else {
				diffLeft.add(left);
				diffRight.add(right);
				continue;
			}
			for (int j = 0; j < left.getVars().size(); j++) {
				varmap.put(Var.alloc(left.getVars().get(j)), Var.alloc(right.getVars().get(j)));
			}
		}
		return new Pair<BasicPattern, PairOfSameType<BasicPattern>>(overlap, new PairOfSameType<BasicPattern>(diffLeft, diffRight));
	}

}
