package org.apache.spark.sql.types
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

object DataTypeUtil {
    /*
	   * The source code below is extracted from Apache Spark v2.4
  		 * [DecimalType]
	   */
    val ByteDecimal: DecimalType = DecimalType(3, 0)
    val ShortDecimal: DecimalType = DecimalType(5, 0)
    val IntDecimal: DecimalType = DecimalType(10, 0)
    val LongDecimal: DecimalType = DecimalType(20, 0)
    val FloatDecimal: DecimalType = DecimalType(14, 7)
    val DoubleDecimal: DecimalType = DecimalType(30, 15)
    val BigIntDecimal: DecimalType = DecimalType(38, 0)

    def DecimalForType(dataType: DataType): DecimalType = dataType match {
      case ByteType => ByteDecimal
      case ShortType => ShortDecimal
      case IntegerType => IntDecimal
      case LongType => LongDecimal
      case FloatType => FloatDecimal
      case DoubleType => DoubleDecimal
  }
    
}