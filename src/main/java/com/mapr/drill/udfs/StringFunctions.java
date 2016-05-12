/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mapr.drill.udfs;

import javax.inject.Inject;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.VarCharHolder;

import io.netty.buffer.DrillBuf;

public class StringFunctions{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StringFunctions.class);

  private StringFunctions() {}

  /*
   * String Function Implementation.
   */

  @FunctionTemplate(name = "split", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Split implements DrillSimpleFunc {
    @Param  VarCharHolder input;
    @Param  VarCharHolder delimiter;

    @Workspace char splitChar;
    @Inject DrillBuf buffer;

    @Output org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter writer;

    @Override
    public void setup() {
      int len = delimiter.end - delimiter.start;
      if (len != 1) {
        throw new IllegalArgumentException("Only single character delimiters are supportted for split()");
      }
      splitChar = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.
          toStringFromUTF8(delimiter.start, delimiter.end, delimiter.buffer).charAt(0);
    }

    @Override
    public void eval() {
      Iterable<String> tokens = com.google.common.base.Splitter.on(splitChar).split(
          org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(input.start, input.end, input.buffer));
      org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter list = writer.rootAsList();
      list.startList();
      for (String token : tokens) {
        final byte[] strBytes = token.getBytes(com.google.common.base.Charsets.UTF_8);
        buffer = buffer.reallocIfNeeded(strBytes.length);
        buffer.setBytes(0, strBytes);
        list.varChar().writeVarChar(0, strBytes.length, buffer);
      }
      list.endList();
    }

  }
}
