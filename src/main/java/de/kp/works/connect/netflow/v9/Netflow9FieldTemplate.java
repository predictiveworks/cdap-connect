package de.kp.works.connect.netflow.v9;
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

public class Netflow9FieldTemplate {

	  private final Netflow9FieldType type;

	  private final int typeId;
	  private final int length;

	  public Netflow9FieldTemplate(int typeId, int length) {
	    this(Netflow9FieldType.getTypeForId(typeId), typeId, length);
	  }

	  public static Netflow9FieldTemplate getScopeFieldTemplate(int scopeTypeId, int length) {
	    return new Netflow9FieldTemplate(Netflow9FieldType.getScopeTypeForId(scopeTypeId), scopeTypeId, length);
	  }

	  public Netflow9FieldTemplate(Netflow9FieldType type, int typeId, int length) {
	    this.type = type;
	    this.typeId = typeId;
	    this.length = length;
	  }

	  public Netflow9FieldType getType() {
	    return type;
	  }

	  public int getTypeId() {
	    return typeId;
	  }

	  public int getLength() {
	    return length;
	  }
	}