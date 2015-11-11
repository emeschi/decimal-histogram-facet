package com.zenobase.search.facet.decimalhistogram;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.HashedBytesArray;


import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.facet.histogram.HistogramFacet;
import org.elasticsearch.search.facet.histogram.HistogramFacet.ComparatorType;
import org.elasticsearch.common.hppc.LongObjectOpenHashMap;

import com.zenobase.search.facet.decimalhistogram.DecimalHistogramFacet;

public class InternalDecimalHistogramFacet extends InternalFacet implements DecimalHistogramFacet {

	private static final BytesReference STREAM_TYPE = new HashedBytesArray(Strings.toUTF8Bytes("decimalHistogram"));

	public static void registerStreams() {
		Streams.registerStream(STREAM, STREAM_TYPE);
	}

	static InternalFacet.Stream STREAM = new Stream() {

		@Override
		public Facet readFacet(StreamInput in) throws IOException {
			return readDecimalHistogramFacet(in);
		}
	};

	@Override
	public BytesReference streamType() {
		return STREAM_TYPE;
	}


	/**
	 * A decimal histogram entry representing a single entry within the result of a
	 * decimal histogram facet.
	 */
	public static class DecimalEntry implements DecimalHistogramFacet.Entry {

		long key;
		long count;
		double binContent;
		double sumOfSquares;

		public DecimalEntry(long key, long count, double binContent, double sumOfSquares) {
			this.key = key;
			this.binContent = binContent;
			this.sumOfSquares = sumOfSquares;
			this.count = count;
		}
		
		@Override
		public long getKey() {
			return key;
		}

		@Override
		public double getKey(double interval) {
			return key * interval;
		}

		@Override
		public long getCount() {
			return count;
		}

		public double getBinContent() {
			return binContent;
		}

		public double getSumOfSquares() {
			return sumOfSquares;
		}
		
		@Override
		public double getTotal() {
			return Double.NaN;
		}

		@Override
		public long getTotalCount() {
			return 0;
		}

		@Override
		public double getMean() {
			return Double.NaN;
		}

		@Override
		public double getMin() {
			return Double.NaN;
		}

		@Override
		public double getMax() {
			return Double.NaN;
		}
		
		public DecimalEntry add(DecimalEntry b){
			this.count += b.count;
			this.binContent += b.binContent;
			this.sumOfSquares += b.sumOfSquares;
			return this;
		}
		
	}

	double interval;
	double offset;
	int nbins;
	double xmin;
	double xmax;
	double integral;
	long nentries;
	double rms;
	double underflows;
	double overflows;
	double mean;
	HistogramFacet.ComparatorType comparatorType;
	DecimalEntry[] entries;

	InternalDecimalHistogramFacet() {

	}

	public InternalDecimalHistogramFacet(String name, int nbins, double xmin, double xmax, HistogramFacet.ComparatorType comparatorType, DecimalEntry[] entries) {
		super(name);
		this.interval = (xmax-xmin)/nbins;
		this.nbins = nbins;
		this.xmin = xmin;
		this.xmax = xmax;
		this.offset = 0.;
		this.comparatorType = comparatorType;
		this.entries = entries;
		this.nentries = 0;
		this.rms = 0.;
		this.integral = 0.;
		this.underflows = 0.;
		this.overflows = 0.;
		this.mean = 0.;
		recalculate();
		
	}

	public InternalDecimalHistogramFacet(String name, int nbins, double xmin, double xmax, HistogramFacet.ComparatorType comparatorType, List<DecimalEntry> entries) {
		super(name);
		this.interval = (xmax-xmin)/nbins;
		this.nbins = nbins;
		this.xmin = xmin;
		this.xmax = xmax;
		this.offset = 0.;
		this.comparatorType = comparatorType;
		this.entries = (DecimalEntry[])entries.toArray();
		this.nentries = 0;
		this.rms = 0.;
		this.integral = 0.;
		this.underflows = 0.;
		this.overflows = 0.;
		this.mean = 0.;
		recalculate();	

		
	}

	
	public InternalDecimalHistogramFacet(String name, double interval, double offset, HistogramFacet.ComparatorType comparatorType, DecimalEntry[] entries) {
		super(name);
		this.nbins = 0;
		this.xmin=1.;
		this.xmax=-1.;
		this.interval = interval;
		this.offset = offset;
		this.comparatorType = comparatorType;
		this.entries = entries;
		this.nentries = 0;
		this.rms = 0.;
		this.integral = 0.;
		this.underflows = 0.;
		this.overflows = 0.;
		this.mean = 0.;
		recalculate();


	}

	public InternalDecimalHistogramFacet(String name, double interval, double offset, HistogramFacet.ComparatorType comparatorType, List<DecimalEntry> entries) {
		super(name);
		this.interval = interval;
		this.offset = offset;
		this.comparatorType = comparatorType;
		this.entries = (DecimalEntry[])entries.toArray();
		this.nentries = 0;
		this.rms = 0.;
		this.integral = 0.;
		this.underflows = 0.;
		this.overflows = 0.;
		this.mean = 0.;
		recalculate();

	}	
	
	public void recalculate(){
		for (int i = (this.nbins==0?0:1); i < entries.length-(this.nbins==0?0:1); ++i) {

				this.nentries += entries[i].getCount();
				this.integral += entries[i].getBinContent();
				this.mean += (entries[i].key*interval+xmin)*entries[i].getBinContent();
			}
		
		if(nbins!=0){
				if(entries.length!=0){
						this.underflows=entries[0].getBinContent();
						this.overflows=entries[entries.length-1].getBinContent();
				}
			}
		this.mean /= this.integral;
		for (int i = (this.nbins==0?0:1); i < entries.length-(this.nbins==0?0:1); ++i) {
				this.rms += ((entries[i].key*interval+xmin)*entries[i].getBinContent()-this.mean)*
						((entries[i].key*interval+xmin)*entries[i].getBinContent()-this.mean);
			}
		
		this.rms = Math.sqrt(this.rms);
		this.rms /= this.integral;
	}
	
	@Override
	public String getType() {
		return TYPE;
	}

	@Override
	public List<DecimalEntry> getEntries() {
		return Arrays.asList(entries);
	}

	@Override
	public Facet reduce(ReduceContext context) {
		List<Facet> facets = context.facets();
		if (facets.size() == 1) {
			InternalDecimalHistogramFacet facet = (InternalDecimalHistogramFacet) facets.get(0);
			Arrays.sort(facet.entries, facet.comparatorType.comparator());
			return facet;
		}

		Recycler.V<LongObjectOpenHashMap<DecimalEntry>> counts = context.cacheRecycler().longObjectMap(-1);
		for (Facet facet : facets) {
			InternalDecimalHistogramFacet histoFacet = (InternalDecimalHistogramFacet) facet;
			for (DecimalEntry entry : histoFacet.entries) {

				if(counts.v().containsKey(entry.getKey()))
					counts.v().put(entry.getKey(),counts.v().get(entry.getKey()).add(entry));
				else
					counts.v().put(entry.getKey(), entry);
			}
		}
		if(!counts.v().containsKey(Integer.MIN_VALUE))
			counts.v().put(Integer.MIN_VALUE, new DecimalEntry(Integer.MIN_VALUE,0,0.,0.));
		if(!counts.v().containsKey(Integer.MAX_VALUE))
			counts.v().put(Integer.MAX_VALUE, new DecimalEntry(Integer.MAX_VALUE,0,0.,0.));
		final boolean[] states = counts.v().allocated;
		final long[] keys = counts.v().keys;
		final Object[] values = counts.v().values;
		DecimalEntry[] localentries = new DecimalEntry[counts.v().size()];
		int entryIndex = 0;
		for (int i = (this.nbins==0?0:1); i < states.length-(this.nbins==0?0:1); ++i) {
			if (states[i]) {
				DecimalEntry entry = (DecimalEntry)values[i];
				localentries[entryIndex++] = new DecimalEntry(keys[i], entry.getCount(),
							entry.getBinContent(), entry.getSumOfSquares());
				this.nentries += entry.getCount();
				this.integral += entry.getBinContent();
				this.mean += (entry.key*interval+xmin)*entry.getBinContent();
			}
		}
		if(nbins!=0){
			if(states[0]){
				DecimalEntry entry = (DecimalEntry)values[0];
				this.underflows=entry.getBinContent();
			}
			if(states[states.length-1]){
				DecimalEntry entry = (DecimalEntry)values[states.length-1];
				this.overflows=entry.getBinContent();
			}
		}
		this.mean /= this.integral;
		for (int i = (this.nbins==0?0:1); i < states.length-(this.nbins==0?0:1); ++i) {
			if (states[i]) {
				DecimalEntry entry = (DecimalEntry)values[i];
				this.rms += ((entry.key*interval+xmin)*this.interval*entry.getBinContent()-this.mean)*
						((entry.key*interval+xmin)*entry.getBinContent()-this.mean);
			}
		}
		this.rms = Math.sqrt(this.rms);
		this.rms/=this.integral;
		counts.close();
		Arrays.sort(localentries, comparatorType.comparator());
		if(nbins==0)
			return new InternalDecimalHistogramFacet(getName(), interval, offset, comparatorType, localentries);
		else
			return new InternalDecimalHistogramFacet(getName(), nbins, xmin, xmax, comparatorType, localentries);
	}

	private interface Fields {

		final XContentBuilderString _TYPE = new XContentBuilderString("_type");
		final XContentBuilderString ENTRIES = new XContentBuilderString("entries");
		final XContentBuilderString BINSIZE = new XContentBuilderString("binsize");
		final XContentBuilderString NBIN = new XContentBuilderString("nbin");
		final XContentBuilderString INTEGRAL = new XContentBuilderString("integral");
		final XContentBuilderString MEAN = new XContentBuilderString("mean");
		final XContentBuilderString RMS = new XContentBuilderString("rms");
		final XContentBuilderString UNDERFLOWS = new XContentBuilderString("underflows");
		final XContentBuilderString OVERFLOWS = new XContentBuilderString("overflows");
		final XContentBuilderString BIN = new XContentBuilderString("bin");
		final XContentBuilderString KEY = new XContentBuilderString("xLow");
		final XContentBuilderString COUNT = new XContentBuilderString("count");
		final XContentBuilderString BINCONTENT = new XContentBuilderString("binContent");
		final XContentBuilderString ERROR = new XContentBuilderString("error");
	}

	@Override
	public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
		builder.startObject(getName());
		builder.field(Fields._TYPE, DecimalHistogramFacet.TYPE);
		builder.field(Fields.BINSIZE, interval);
		builder.field(Fields.NBIN, nbins);
		builder.field(Fields.INTEGRAL, integral);
		builder.field(Fields.MEAN, mean);
		builder.field(Fields.RMS,rms);
		builder.field(Fields.UNDERFLOWS, underflows);
		builder.field(Fields.OVERFLOWS, overflows);
		builder.startArray(Fields.ENTRIES);
		for (int i = (nbins==0?0:1); i < (nbins==0?entries.length:entries.length-1); i++) {
			toXContent(entries[i], builder);
		}
		builder.endArray();
		builder.endObject();
		return builder;
	}

	private void toXContent(InternalDecimalHistogramFacet.DecimalEntry entry, XContentBuilder builder) throws IOException {
		builder.startObject();
		builder.field(Fields.BIN, entry.getKey());
		builder.field(Fields.KEY, entry.getKey() * interval+xmin);
		builder.field(Fields.COUNT, entry.getCount());
		builder.field(Fields.BINCONTENT, entry.getBinContent());
		builder.field(Fields.ERROR, Math.sqrt(entry.getSumOfSquares()));
		builder.endObject();
	}

	public static InternalDecimalHistogramFacet readDecimalHistogramFacet(StreamInput in) throws IOException {
		InternalDecimalHistogramFacet facet = new InternalDecimalHistogramFacet();
		facet.readFrom(in);
		return facet;
	}

	@Override
	public void readFrom(StreamInput in) throws IOException {
		super.readFrom(in);
		comparatorType = ComparatorType.fromId(in.readByte());
		interval = in.readDouble();
		nbins = in.readInt();
		int size = in.readVInt();
		integral = in.readDouble();
		mean = in.readDouble();
		rms = in.readDouble();
		underflows = in.readDouble();
		overflows = in.readDouble();
		entries = new DecimalEntry[size];
		for (int i = (nbins==0?0:1); i < (nbins==0?size:size-1); i++) {
			entries[i] = new DecimalEntry(in.readLong(), in.readVLong(), in.readDouble(), in.readDouble());
		}
	}

	@Override
	public void writeTo(StreamOutput out) throws IOException {
		super.writeTo(out);
		out.writeByte(comparatorType.id());
		out.writeDouble(interval);
		out.writeInt(nbins);
		out.writeVInt(entries.length);
		out.writeDouble(integral);
		out.writeDouble(mean);
		out.writeDouble(rms);
		out.writeDouble(underflows);
		out.writeDouble(overflows);
		for (int i = (nbins==0?0:1); i < (nbins==0?entries.length:entries.length-1); i++) {
			out.writeLong(entries[i].key);
			out.writeLong(entries[i].key);
			out.writeVLong(entries[i].count);
			out.writeDouble(entries[i].binContent);
			out.writeDouble(Math.sqrt(entries[i].sumOfSquares));
		}
	}
}
