package org.apache.jena.sparql.engine.join;

import java.util.PriorityQueue;
import java.util.Queue;

import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.join.QueryIterSimJoin.Neighbor;

public abstract class KNNSimJoinSolver extends SimJoinSolver {
	
	protected Queue<Neighbor<Binding>> cache = new PriorityQueue<QueryIterSimJoin.Neighbor<Binding>>(Neighbor.comparator);
	protected Binding currentSource = null;

	public KNNSimJoinSolver(QueryIterSimJoin simjoin) {
		super(simjoin);
	}
	
	protected abstract void getNextBatch(Binding l);
	
	@Override
	public Binding nextBinding() {
		if (cache.isEmpty() && simjoin.getLeft().hasNext()) {
			currentSource = simjoin.getLeft().nextBinding();
			getNextBatch(currentSource);
		}
		Binding r = consolidateKNN(currentSource, cache.poll());
		return r;
	}

	@Override
	public boolean hasNextBinding() {
		return (!cache.isEmpty()) || simjoin.getLeft().hasNext();
	}

}
