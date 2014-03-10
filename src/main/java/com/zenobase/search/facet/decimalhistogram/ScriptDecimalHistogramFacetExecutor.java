package com.zenobase.search.facet.decimalhistogram;

import java.io.IOException;
import java.util.Map;

import com.carrotsearch.hppc.LongObjectOpenHashMap;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.facet.histogram.HistogramFacet;
import org.elasticsearch.search.facet.histogram.HistogramFacet.ComparatorType;
import org.elasticsearch.search.internal.SearchContext;

public class ScriptDecimalHistogramFacetExecutor extends FacetExecutor {

	final SearchScript keyScript;
    final SearchScript valueScript;
    final double interval;
    final double offset;
    
    final Recycler.V<LongObjectOpenHashMap<InternalDecimalHistogramFacet.DecimalEntry>> entries;
    
    private final HistogramFacet.ComparatorType comparatorType;

	public ScriptDecimalHistogramFacetExecutor(String scriptLang,
			String keyScript, String valueScript, Map<String, Object> params, double interval,
			double offset, ComparatorType comparatorType, SearchContext context) {
	       this.keyScript = context.scriptService().search(context.lookup(), scriptLang, keyScript, params);
	       this.valueScript = context.scriptService().search(context.lookup(), scriptLang, valueScript, params);
	       this.interval = interval > 0. ? interval : 0.;
	       this.offset = offset;
	       this.entries = context.cacheRecycler().longObjectMap(-1);
	       
	       this.comparatorType = comparatorType;
	       


	}
	@Override
	public FacetExecutor.Collector collector() {
		return new Collector(entries.v());
	}

	@Override
	public InternalFacet buildFacet(String facetName) {
        InternalDecimalHistogramFacet.DecimalEntry[] entries1 = new InternalDecimalHistogramFacet.DecimalEntry[entries.v().size()];
        final boolean[] states = entries.v().allocated;
        final Object[] values = entries.v().values;
        int j = 0;
        for (int i = 0; i < states.length; i++) {
            if (states[i]) {
            	InternalDecimalHistogramFacet.DecimalEntry value = (InternalDecimalHistogramFacet.DecimalEntry)values[i];
                entries1[j]=value;
                j++;
            }
//            else
//            	entries1[i]= new InternalDecimalHistogramFacet.DecimalEntry(i,0.0);
        }

        entries.release();
		return new InternalDecimalHistogramFacet(facetName, interval, offset, comparatorType, entries1);
	}

	private class Collector extends FacetExecutor.Collector {

        final LongObjectOpenHashMap<InternalDecimalHistogramFacet.DecimalEntry> entries;
		
		public Collector(LongObjectOpenHashMap<InternalDecimalHistogramFacet.DecimalEntry> entries) {
			this.entries = entries;
		}

		@Override
		public void setScorer(Scorer scorer) throws IOException {
			keyScript.setScorer(scorer);
			valueScript.setScorer(scorer);
		}

		
		@Override
		public void setNextReader(AtomicReaderContext context) throws IOException {
            keyScript.setNextReader(context);
            valueScript.setNextReader(context);
		}

		@Override
		public void collect(int doc) throws IOException {
            keyScript.setNextDocId(doc);
            valueScript.setNextDocId(doc);

            long bucket = (long) Math.floor(((keyScript.runAsDouble() + offset) / interval));             
            double value = valueScript.runAsDouble();

            InternalDecimalHistogramFacet.DecimalEntry entry = entries.get(bucket);
            if (entry == null) {
                entry = new InternalDecimalHistogramFacet.DecimalEntry(bucket,1,value, value*value);
                entries.put(bucket, entry);
            } else {
                entry.binContent+=value;
                entry.sumOfSquares+=value*value;
                entry.count++;
//                entry.total += value;
//                if (value < entry.min) {
//                    entry.min = value;
//                }
//                if (value > entry.max) {
//                    entry.max = value;
//                }
            }
		}

		@Override
		public void postCollection() {

		}
	}


}
