package com.zenobase.search.facet.decimalhistogram;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.common.hppc.LongLongOpenHashMap;
import org.elasticsearch.common.hppc.LongObjectMap;
import org.elasticsearch.common.hppc.LongObjectOpenHashMap;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.index.fielddata.AtomicNumericFieldData;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.search.facet.DoubleFacetAggregatorBase;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.facet.histogram.HistogramFacet.ComparatorType;
import org.elasticsearch.search.internal.SearchContext;

import com.zenobase.search.facet.decimalhistogram.InternalDecimalHistogramFacet.DecimalEntry;


public class DecimalHistogramFacetExecutor extends FacetExecutor {

	private final IndexNumericFieldData<AtomicNumericFieldData> indexFieldData;
	private final ComparatorType comparatorType;
	private final double interval;
	private final double offset;

	final Recycler.V<LongObjectOpenHashMap<InternalDecimalHistogramFacet.DecimalEntry>> counts;

	public DecimalHistogramFacetExecutor(IndexNumericFieldData<AtomicNumericFieldData> indexFieldData, double interval, double offset, ComparatorType comparatorType, SearchContext context) {
		this.indexFieldData = indexFieldData;
		this.interval = interval;
		this.offset = offset;
		this.comparatorType = comparatorType;
		this.counts = context.cacheRecycler().longObjectMap(-1);
	}

	@Override
	public FacetExecutor.Collector collector() {
		return new Collector();
	}

	@Override
	public InternalFacet buildFacet(String facetName) {
		InternalDecimalHistogramFacet.DecimalEntry[] entries = new InternalDecimalHistogramFacet.DecimalEntry[counts.v().size()];
		final boolean[] states = counts.v().allocated;
		final long[] keys = counts.v().keys;
		final DecimalEntry[] values = counts.v().values;
		int entryIndex = 0;
		for (int i = 0; i < states.length; ++i) {
			if (states[i]) {
				entries[entryIndex++] = new InternalDecimalHistogramFacet.DecimalEntry(keys[i], values[i].getCount(), 
						values[i].getBinContent(), values[i].getSumOfSquares());
			}
		}
		counts.release();
		return new InternalDecimalHistogramFacet(facetName, interval, offset, comparatorType, entries);
	}

	private class Collector extends FacetExecutor.Collector {

		private final HistogramProc histoProc;
		private DoubleValues values;

		public Collector() {
			this.histoProc = new HistogramProc(interval, offset, counts.v());
		}

		@Override
		public void setNextReader(AtomicReaderContext context) throws IOException {
			values = indexFieldData.load(context).getDoubleValues();
		}

		@Override
		public void collect(int doc) throws IOException {
			histoProc.onDoc(doc, values);
		}

		@Override
		public void postCollection() {

		}
	}

	private static class HistogramProc extends DoubleFacetAggregatorBase {

		private final double interval;
		private final double offset;
		private final LongObjectOpenHashMap<DecimalEntry> counts;

		public HistogramProc(double interval, double offset, LongObjectOpenHashMap<DecimalEntry> counts) {
			this.interval = interval;
			this.offset = offset;
			this.counts = counts;
		}

		@Override
		public void onValue(int docId, double value) {
			long bucket = (long) Math.floor(((value + offset) / interval));
            DecimalEntry entry = counts.get(bucket);
            if (entry == null) {
                entry = new InternalDecimalHistogramFacet.DecimalEntry(bucket,1,value, value*value);
                counts.put(bucket, entry);
            } else {
                entry.count++;
                entry.binContent += value;
                entry.sumOfSquares += value*value;
            }
		}
	}
}
