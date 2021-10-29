package de.kp.works.connect.aerospike;
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Writable;

import com.aerospike.client.Value;
import com.aerospike.client.util.Packer;
import com.aerospike.client.util.Unpacker.ObjectUnpacker;

public class AerospikeEntry {

	public Key newKey() {
		return new Key();
	}

	public Key newKey(com.aerospike.client.Key key) {
		return new Key(key);
	}

	public Record newRecord() {
		return new Record();
	}

	public Record newRecord(com.aerospike.client.Record record) {
		return new Record(record);
	}

	public static class Key implements Writable {

		private com.aerospike.client.Key key;

		public Key() {
		}

		public Key(com.aerospike.client.Key key) {
			this.key = key;
		}

		public com.aerospike.client.Key getKey() {
			return key;
		}

		@Override
		public void readFields(DataInput in) throws IOException {

			String namespace;
			String setName;

			byte[] digest;
			Value userKey = null;

			try {

				namespace = in.readUTF();
				setName = in.readUTF();

				int digestLen = in.readInt();
				digest = new byte[digestLen];
				in.readFully(digest);

				if (in.readBoolean()) {
					int buflen = in.readInt();
					byte[] buff = new byte[buflen];
					in.readFully(buff);
					ObjectUnpacker unpack = new ObjectUnpacker(buff, 0, buff.length);
					userKey = Value.get(unpack.unpackObject());
				}

				key = new com.aerospike.client.Key(namespace, digest, setName, userKey);

			} catch (Exception e) {
				throw new IOException(e);
			}

		}

		@Override
		public void write(DataOutput out) throws IOException {

			String namespace = key.namespace;
			String setName = key.setName;

			byte[] digest = key.digest;
			Value userKey = key.userKey;

			try {

				out.writeUTF(namespace);
				out.writeUTF(setName);

				out.writeInt(digest.length);
				out.write(digest);
				out.writeBoolean(userKey != null);

				if (userKey == null) {
					out.writeBoolean(false);

				} else {

					out.writeBoolean(true);

					Packer pack = new Packer();
					pack.packObject(userKey);

					byte[] buff = pack.toByteArray();

					out.writeInt(buff.length);
					out.write(buff);
				}

			} catch (Exception e) {
				throw new IOException(e);
			}

		}

	}

	public static class Record implements Writable {

		private com.aerospike.client.Record record;

		public Record() {
		}

		public Record(com.aerospike.client.Record record) {
			this.record = record;
		}

		public com.aerospike.client.Record getRecord() {
			return record;
		}

		@Override
		public void readFields(DataInput in) throws IOException {

			Map<String, Object> bins;
			int generation;
			int expiration;

			try {
				generation = in.readInt();
				expiration = in.readInt();

				int nbins = in.readInt();
				bins = new HashMap<>();
				for (int i = 0; i < nbins; ++i) {

					String key = in.readUTF();
					int buflen = in.readInt();
					byte[] buff = new byte[buflen];
					in.readFully(buff);

					ObjectUnpacker unpack = new ObjectUnpacker(buff, 0, buff.length);
					Object obj = unpack.unpackObject();

					bins.put(key, obj);

				}

				record = new com.aerospike.client.Record(bins, generation, expiration);

			} catch (Exception e) {
				throw new IOException(e);

			}

		}

		@Override
		public void write(DataOutput out) throws IOException {

			Map<String, Object> bins = record.bins;
			int generation = record.generation;
			int expiration = record.expiration;

			try {

				out.writeInt(generation);
				out.writeInt(expiration);
				out.writeInt(bins.size());

				for (Map.Entry<String, Object> entry : bins.entrySet()) {

					out.writeUTF(entry.getKey());

					Packer pack = new Packer();
					pack.packObject(entry.getValue());

					byte[] buff = pack.toByteArray();

					out.writeInt(buff.length);
					out.write(buff);

				}
			} catch (Exception e) {
				throw new IOException(e);

			}

		}

	}
}
