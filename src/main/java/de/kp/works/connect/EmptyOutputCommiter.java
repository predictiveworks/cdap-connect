package de.kp.works.connect;
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

import java.io.IOException;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class EmptyOutputCommiter extends OutputCommitter {

	@Override
	public void abortTask(TaskAttemptContext context) throws IOException {
	}

	@Override
	public void commitTask(TaskAttemptContext context) throws IOException {
	}

	@Override
	public boolean needsTaskCommit(TaskAttemptContext context) throws IOException {
		return false;
	}

	@Override
	public void setupJob(JobContext context) throws IOException {
	}

	@Override
	public void setupTask(TaskAttemptContext context) throws IOException {
	}

}
