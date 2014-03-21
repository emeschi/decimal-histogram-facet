package com.zenobase.search.facet.decimalhistogram;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.AtomicNumericFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.FacetExecutor.Mode;
import org.elasticsearch.search.facet.FacetParser;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;

import org.elasticsearch.search.facet.histogram.HistogramFacet.ComparatorType;
import org.elasticsearch.search.internal.SearchContext;

import com.zenobase.search.facet.decimalhistogram.DecimalHistogramFacet;

public class DecimalHistogramFacetParser extends AbstractComponent implements FacetParser {

	@Inject
	public DecimalHistogramFacetParser(Settings settings) {
		super(settings);
		InternalDecimalHistogramFacet.registerStreams();
	}

	@Override
	public String[] types() {
		return new String[] {
			DecimalHistogramFacet.TYPE
		};
	}

	@Override
	public Mode defaultMainMode() {
		return FacetExecutor.Mode.COLLECTOR;
	}

	@Override
	public Mode defaultGlobalMode() {
		return FacetExecutor.Mode.COLLECTOR;
	}

	@Override
	public FacetExecutor parse(String facetName, XContentParser parser, SearchContext context) throws IOException {

		String field = null;
        String value = null;
        String keyScript = null;
        String valueScript = null;
        String scriptLang = null;
 
        Map<String, Object> params = null;
        
		double interval = 0.0;
		double offset = 0.0;
		int nbin = 0;
		double xmin = 1.;
		double xmax = -1.;
		ComparatorType comparatorType = ComparatorType.KEY;
 
		
		String currentName = parser.currentName();
		XContentParser.Token token;
		while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
			if (token == XContentParser.Token.FIELD_NAME) {
				currentName = parser.currentName();
			} else if (token == XContentParser.Token.START_OBJECT) {
                if ("params".equals(currentName)) {
                    params = parser.map();
                }
            } else if (token.isValue()) {
				if ("field".equals(currentName)) {
					field = parser.text();
				} else if ("interval".equals(currentName)) {
					interval = parser.doubleValue();
				} else if ("nbin".equals(currentName)) {
					nbin = parser.intValue();
				} else if ("xmin".equals(currentName)) {
					xmin = parser.doubleValue();
				} else if ("xmax".equals(currentName)) {
					xmax = parser.doubleValue();
				} else if ("key_field".equals(currentName) || "keyField".equals(currentName)) {
                    field = parser.text();
                } else if ("value_field".equals(currentName) || "valueField".equals(currentName)) {
                    value = parser.text();
                } else if ("key_script".equals(currentName) || "keyScript".equals(currentName)) {
                    keyScript = parser.text();
                } else if ("value_script".equals(currentName) || "valueScript".equals(currentName)) {
                    valueScript = parser.text();
                } else if ("offset".equals(currentName)) {
					offset = parser.doubleValue();
				} else if ("order".equals(currentName) || "comparator".equals(currentName)) {
					comparatorType = ComparatorType.fromString(parser.text());
				} else if ("lang".equals(currentName)) {
                    scriptLang = parser.text();
                }
			}
		}

		if (keyScript != null && valueScript != null) {
			if(nbin==0)
				return new ScriptDecimalHistogramFacetExecutor(scriptLang, keyScript, valueScript, params, interval, offset, comparatorType, context);
			else
				return new ScriptDecimalHistogramFacetExecutor(scriptLang, keyScript, valueScript, params, nbin, xmin, xmax, comparatorType, context);
		}
        if (field == null) {
			throw new FacetPhaseExecutionException(facetName, "[field] is required for decimal histogram facet");
		}
		if (interval <= 0.0 && nbin == 0) {
			throw new FacetPhaseExecutionException(facetName, "[interval] must be greater than 0.0");
		}
		FieldMapper<AtomicNumericFieldData> fieldMapper = context.smartNameFieldMapper(field);
        if (fieldMapper == null) {
            throw new FacetPhaseExecutionException(facetName, "failed to find mapping for [" + field + "]");
        }
        IndexNumericFieldData<AtomicNumericFieldData> indexFieldData = context.fieldData().getForField(fieldMapper);
        if(nbin==0)
        	return new DecimalHistogramFacetExecutor(indexFieldData, interval, offset, comparatorType, context);
        else
        	return new DecimalHistogramFacetExecutor(indexFieldData, nbin, xmin, xmax, comparatorType, context);
	}
}
