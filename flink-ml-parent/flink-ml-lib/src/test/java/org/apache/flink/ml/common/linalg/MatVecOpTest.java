/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.common.linalg;

import org.junit.Before;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test cases for {@link MatVecOp}. */
public class MatVecOpTest {
    private static final double TOL = 1.0e-6;
    private DenseVector dv;
    private SparseVector sv;

    @Before
    public void setUp() throws Exception {
        dv = new DenseVector(new double[] {1, 2, 3, 4});
        sv = new SparseVector(4, new int[] {0, 2}, new double[] {1., 1.});
    }

    @Test
    public void testPlus() throws Exception {
        Vector plusResult1 = MatVecOp.plus(dv, sv);
        Vector plusResult2 = MatVecOp.plus(sv, dv);
        Vector plusResult3 = MatVecOp.plus(sv, sv);
        Vector plusResult4 = MatVecOp.plus(dv, dv);
        Assertions.assertTrue(plusResult1 instanceof DenseVector);
        Assertions.assertTrue(plusResult2 instanceof DenseVector);
        Assertions.assertTrue(plusResult3 instanceof SparseVector);
        Assertions.assertTrue(plusResult4 instanceof DenseVector);
        Assertions.assertArrayEquals(
                ((DenseVector) plusResult1).getData(), new double[] {2, 2, 4, 4}, TOL);
        Assertions.assertArrayEquals(
                ((DenseVector) plusResult2).getData(), new double[] {2, 2, 4, 4}, TOL);
        Assertions.assertArrayEquals(((SparseVector) plusResult3).getIndices(), new int[] {0, 2});
        Assertions.assertArrayEquals(
                ((SparseVector) plusResult3).getValues(), new double[] {2., 2.}, TOL);
        Assertions.assertArrayEquals(
                ((DenseVector) plusResult4).getData(), new double[] {2, 4, 6, 8}, TOL);
    }

    @Test
    public void testMinus() throws Exception {
        Vector minusResult1 = MatVecOp.minus(dv, sv);
        Vector minusResult2 = MatVecOp.minus(sv, dv);
        Vector minusResult3 = MatVecOp.minus(sv, sv);
        Vector minusResult4 = MatVecOp.minus(dv, dv);
        Assertions.assertTrue(minusResult1 instanceof DenseVector);
        Assertions.assertTrue(minusResult2 instanceof DenseVector);
        Assertions.assertTrue(minusResult3 instanceof SparseVector);
        Assertions.assertTrue(minusResult4 instanceof DenseVector);
        Assertions.assertArrayEquals(
                ((DenseVector) minusResult1).getData(), new double[] {0, 2, 2, 4}, TOL);
        Assertions.assertArrayEquals(
                ((DenseVector) minusResult2).getData(), new double[] {0, -2, -2, -4}, TOL);
        Assertions.assertArrayEquals(((SparseVector) minusResult3).getIndices(), new int[] {0, 2});
        Assertions.assertArrayEquals(
                ((SparseVector) minusResult3).getValues(), new double[] {0., 0.}, TOL);
        Assertions.assertArrayEquals(
                ((DenseVector) minusResult4).getData(), new double[] {0, 0, 0, 0}, TOL);
    }

    @Test
    public void testDot() throws Exception {
        Assertions.assertEquals(MatVecOp.dot(dv, sv), 4.0, TOL);
        Assertions.assertEquals(MatVecOp.dot(sv, dv), 4.0, TOL);
        Assertions.assertEquals(MatVecOp.dot(sv, sv), 2.0, TOL);
        Assertions.assertEquals(MatVecOp.dot(dv, dv), 30.0, TOL);
    }

    @Test
    public void testSumAbsDiff() throws Exception {
        Assertions.assertEquals(MatVecOp.sumAbsDiff(dv, sv), 8.0, TOL);
        Assertions.assertEquals(MatVecOp.sumAbsDiff(sv, dv), 8.0, TOL);
        Assertions.assertEquals(MatVecOp.sumAbsDiff(sv, sv), 0.0, TOL);
        Assertions.assertEquals(MatVecOp.sumAbsDiff(dv, dv), 0.0, TOL);
    }

    @Test
    public void testSumSquaredDiff() throws Exception {
        Assertions.assertEquals(MatVecOp.sumSquaredDiff(dv, sv), 24.0, TOL);
        Assertions.assertEquals(MatVecOp.sumSquaredDiff(sv, dv), 24.0, TOL);
        Assertions.assertEquals(MatVecOp.sumSquaredDiff(sv, sv), 0.0, TOL);
        Assertions.assertEquals(MatVecOp.sumSquaredDiff(dv, dv), 0.0, TOL);
    }
}
