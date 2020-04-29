package de.kp.works.connect.webhose;
/*
 * Copyright (c) 2019 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 * 
 */

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.format.UnexpectedFormatException;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import de.kp.works.connect.http.page.HttpPage;
import de.kp.works.connect.http.error.ErrorStrategy;
import de.kp.works.connect.http.page.HttpEntry;

import org.apache.hadoop.io.NullWritable;

@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("WebhoseSource")
@Description("A batch connector plugin to read structured records from Webhose HTTP endpoint.")
public class WebhoseSource extends BatchSource<NullWritable, HttpPage, StructuredRecord> {

	private final WebhoseConfig config;

	public WebhoseSource(WebhoseConfig config) {
		this.config = config;
	}

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
		/*
		 * Validate configuration when macros are
		 * not yet substituted
		 */
		config.validate();

	}

	@Override
	public void prepareRun(BatchSourceContext context) {
		/*
		 * Validate configuration when macros are
		 * already subsituted
		 */
		config.validate();
		context.setInput(Input.of(config.referenceName, new WebhoseInputFormatProvider(config)));

	}

	@Override
	public void initialize(BatchRuntimeContext context) throws Exception {
		super.initialize(context);
	}

	@Override
	public void transform(KeyValue<NullWritable, HttpPage> input, Emitter<StructuredRecord> emitter) {

		HttpPage page = input.getValue();
		while (page.hasNext()) {
			
			HttpEntry pageEntry = page.next();
			if (!pageEntry.isError()) {
				emitter.emit(pageEntry.getRecord());

			} else {
				/*
				 * The error response strategy is part of the HTTP configuration
				 * and is made available for each page entry
				 */
				InvalidEntry<StructuredRecord> invalidEntry = pageEntry.getError();
				ErrorStrategy strategy = pageEntry.getStrategy();
				
				switch (strategy) {
				case SKIP:
					break;
				case SEND:
					emitter.emitError(invalidEntry);
					break;
				case STOP:
					throw new RuntimeException(invalidEntry.getErrorMsg());
				default:
					throw new UnexpectedFormatException(
							String.format("Unknown error strategy '%s'", strategy.getValue()));
				}
			}
		}
	}
}
