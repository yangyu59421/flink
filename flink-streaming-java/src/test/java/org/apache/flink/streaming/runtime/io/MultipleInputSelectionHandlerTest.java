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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.streaming.api.operators.InputSelection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link MultipleInputSelectionHandler}. */
public class MultipleInputSelectionHandlerTest {
    @Test
    public void testShouldSetAvailableForAnotherInput() {
        InputSelection secondAndThird = new InputSelection.Builder().select(2).select(3).build();

        MultipleInputSelectionHandler selectionHandler =
                new MultipleInputSelectionHandler(() -> secondAndThird, 3);
        selectionHandler.nextSelection();

        assertFalse(selectionHandler.shouldSetAvailableForAnotherInput());

        selectionHandler.setUnavailableInput(0);
        assertFalse(selectionHandler.shouldSetAvailableForAnotherInput());

        selectionHandler.setUnavailableInput(2);
        assertTrue(selectionHandler.shouldSetAvailableForAnotherInput());

        selectionHandler.setAvailableInput(0);
        assertTrue(selectionHandler.shouldSetAvailableForAnotherInput());

        selectionHandler.setAvailableInput(2);
        assertFalse(selectionHandler.shouldSetAvailableForAnotherInput());
    }
}
