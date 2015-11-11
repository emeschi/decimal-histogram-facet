package com.zenobase.search.facet.decimalhistogram;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.facet.DoubleFacetAggregatorBase;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.facet.histogram.HistogramFacet.ComparatorType;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.common.hppc.LongObjectOpenHashMap;

import com.zenobase.search.facet.decimalhistogram.InternalDecimalHistogramFacet.DecimalEntry;


public class DecimalHistogramFacetExecutor extends FacetExecutor {

	private final IndexNumericFieldData indexFieldData;
	private final ComparatorType comparatorType;
	private final double interval;
	private final double offset;
	private final int nbins;
	private final double xmin;
	private final double xmax;

	final Recycler.V<LongObjectOpenHashMap<InternalDecimalHistogramFacet.DecimalEntry>> counts;


	public DecimalHistogramFacetExecutor(IndexNumericFieldData indexFieldData, int nbin, double xmin, double xmax, ComparatorType comparatorType, SearchContext context) {
		this.indexFieldData = indexFieldData;
		this.nbins=nbin; //2 more will be added for underflow and overflow
		this.xmax = xmax;
		this.xmin = xmin;
		
		this.interval = (xmax-xmin)/nbins;
		this.offset = 0.;
		this.comparatorType = comparatorType;
		this.counts = context.cacheRecycler().longObjectMap(-1);
	}
	
	public DecimalHistogramFacetExecutor(IndexNumericFieldData indexFieldData, double interval, double offset, ComparatorType comparatorType, SearchContext context) {
		this.indexFieldData = indexFieldData;
		this.nbins = 0;
		this.xmin=1.;
		this.xmax=-1.; //signal automatic binning
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
		final Object[] values = counts.v().values;
		int entryIndex = 0;
		for (int i = 0; i < states.length; ++i) {
			if (states[i]) {
				InternalDecimalHistogramFacet.DecimalEntry value = (InternalDecimalHistogramFacet.DecimalEntry)values[i];
				entries[entryIndex++] = new InternalDecimalHistogramFacet.DecimalEntry(keys[i], value.getCount(), 
						value.getBinContent(), value.getSumOfSquares());
			}
		}
		counts.release();
		if(nbins==0)
			return new InternalDecimalHistogramFacet(facetName, interval, offset, comparatorType, entries);
		else
			return new InternalDecimalHistogramFacet(facetName, nbins, xmin, xmax, comparatorType, entries);

	}		

	private class Collector extends FacetExecutor.Collector {

		private final HistogramProc histoProc;
		private SortedNumericDoubleValues values;

		public Collector() {
			if(nbins==0)
				this.histoProc = new HistogramProc(interval, offset, counts.v());
			else
				this.histoProc = new HistogramProc(nbins,xmin,xmax, counts.v());
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
		private final int nbins;
		private final double xmin;
		private final double xmax;
		private final LongObjectOpenHashMap<DecimalEntry> counts;

		public HistogramProc(double interval, double offset, LongObjectOpenHashMap<DecimalEntry> counts) {
			this.interval = interval;
			this.offset = offset;
			this.nbins = 0;
			this.xmin=1.;
			this.xmax=-1.;
			this.counts = counts;
		}

		public HistogramProc(int nbins, double xmin, double xmax, LongObjectOpenHashMap<DecimalEntry> counts) {
			this.interval = (xmax-xmin)/nbins;
			this.xmax=xmax;
			this.xmin=xmin;
			this.nbins=nbins;
			this.offset = 0.;
			this.counts = counts;
		}
		
		@Override
		public void onValue(int docId, double value) {
			if(nbins==0){
				long bucket = (long) Math.floor(((value + offset) / interval));
				DecimalEntry entry = counts.get(bucket);
				if (entry == null) {
					entry = new InternalDecimalHistogramFacet.DecimalEntry(bucket,1,1., 1.);
					counts.put(bucket, entry);
				} else {
					entry.count++;
					entry.binContent += 1.0;
					entry.sumOfSquares += 1.0;
				}
			}
			else{
				long bucket = 0;
				if(value>xmax) bucket = Integer.MAX_VALUE;
				else if(value<xmin) bucket = Integer.MIN_VALUE;
				else bucket = (long) Math.floor(((value - xmin) / interval));
				
				DecimalEntry entry = counts.get(bucket);
				if (entry == null) {
					entry = new InternalDecimalHistogramFacet.DecimalEntry(bucket,1,1.,1.);
					counts.put(bucket, entry);
				} else {
					entry.count++;
					entry.binContent += 1.0;
					entry.sumOfSquares += 1.0;
				}
			}
			}
	}
}
