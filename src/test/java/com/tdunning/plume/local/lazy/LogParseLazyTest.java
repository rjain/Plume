/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tdunning.plume.local.lazy;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.tdunning.plume.DoFn;
import com.tdunning.plume.EmitFn;
import com.tdunning.plume.Ordering;
import com.tdunning.plume.PCollection;
import com.tdunning.plume.PTable;
import com.tdunning.plume.Pair;
import com.tdunning.plume.Plume;
import com.tdunning.plume.local.eager.LocalPlume;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;

import static com.tdunning.plume.Plume.strings;
import static org.junit.Assert.*;
import com.tdunning.plume.local.LogParseTest;

/**
 * @author rjain
 * Test to verify  grouping and ordering of elements within each group, using the lazy execution model
 */
public class LogParseLazyTest extends LogParseTest {
  @Test
  public void parseGroupSort() throws IOException {
    Plume p = new LazyPlume();
    PCollection<String> logs = p.fromJava(p.readResourceFile("log.txt"));
    
    // generate key, value pairs for each log statement
    LazyTable<String, Event> events = (LazyTable<String,Event>) p.flatten(logs).map(new DoFn<String, Pair<String, Event>>() {
      @Override
      public void process(String logLine, EmitFn<Pair<String, Event>> emitter) {
    	if (logLine.length()>0) {  
    		Event e = new Event(logLine);
    		emitter.emit(new Pair<String, Event>(e.getName(), e));
    	}
      }
    }, Plume.tableOf(strings(), strings()));

    Ordering<Event> ordering = new Ordering<Event>() {
    	public int compare(Event left, Event right) {
  	      return left.compareTo(right);
       };
    };
    
    // add group by operation with ordering defined
    PTable<String, Iterable<Event>> byName = events.groupByKey(ordering); 
    
    LazyTable<String,Iterable<Event>> byNameImpl = (LazyTable<String,Iterable<Event>>)byName;
    
    Executor executor = new Executor();
    Iterable<Pair<String,Iterable<Event>>>result = executor.execute(byNameImpl);
    
    for (Pair<String,Iterable<Event>>logIter: result) {
    	//String nameKey = logIter.getKey();
    	Iterable<Event> chatEvents = logIter.getValue();
    	// check if ordering indeed happened in the result
    	//System.out.println("key:" + logIter.getKey() + "; value: " + logIter.getValue());
    	assertTrue(ordering.isOrdered(chatEvents));
    }
  }

}
