package org.apache.jena.sparql.engine.iterator;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Vector;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.serializer.SerializationContext;


/**
 * Provides "infinite" copies of a QueryIterator using a shared Buffer.
 * @author scfer
 *
 */
public class BufferedQueryIteratorFactory {
	
	private Vector<Binding> buffer = new Vector<Binding>();
	private QueryIterator qIter;

	public BufferedQueryIteratorFactory(QueryIterator qIter) {
		this.qIter = qIter;
	}
	
	public BufferedQueryIterator createBufferedQueryIterator() {
		return new BufferedQueryIterator(qIter, buffer);
	}
	
	
	public class BufferedQueryIterator extends QueryIteratorBase{
		
		private QueryIterator qIter;
		private List<Binding> buffer;
		private int pos;

		private BufferedQueryIterator(QueryIterator qIter, List<Binding> buffer) {
			this.pos = 0;
			this.qIter = qIter;
			this.buffer = buffer;
		}

		@Override
		public void output(IndentedWriter out, SerializationContext sCxt) {}

		@Override
		protected boolean hasNextBinding() {
			if (this.pos < this.buffer.size()) {
				return true;
			}
			return this.qIter.hasNext();
		}

		@Override
		protected Binding moveToNextBinding() {
			if (!hasNext()) {
				throw new NoSuchElementException("Buffer is empty.");
			}
			if (pos == this.buffer.size()) {
				this.buffer.add(this.qIter.nextBinding());
			}
			return this.buffer.get(pos++);
		}

		@Override
		protected void closeIterator() {}

		@Override
		protected void requestCancel() {}

	}

}
