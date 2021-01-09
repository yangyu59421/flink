/*
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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.ArrayListSerializer;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.internal.InternalKvState.StateIncrementalVisitor;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/** Test for {@link CopyOnWriteStateMap}. */
public class CopyOnWriteStateMapTest extends TestLogger {

    /** Testing the basic map operations. */
    @Test
    public void testPutGetRemoveContainsTransform() throws Exception {
        final CopyOnWriteStateMap<Integer, Integer, ArrayList<Integer>> stateMap =
                new CopyOnWriteStateMap<>(new ArrayListSerializer<>(IntSerializer.INSTANCE));

        ArrayList<Integer> state11 = new ArrayList<>();
        state11.add(41);
        ArrayList<Integer> state21 = new ArrayList<>();
        state21.add(42);
        ArrayList<Integer> state12 = new ArrayList<>();
        state12.add(43);

        Assertions.assertNull(stateMap.putAndGetOld(1, 1, state11));
        Assertions.assertEquals(state11, stateMap.get(1, 1));
        Assertions.assertEquals(1, stateMap.size());

        Assertions.assertNull(stateMap.putAndGetOld(2, 1, state21));
        Assertions.assertEquals(state21, stateMap.get(2, 1));
        Assertions.assertEquals(2, stateMap.size());

        Assertions.assertNull(stateMap.putAndGetOld(1, 2, state12));
        Assertions.assertEquals(state12, stateMap.get(1, 2));
        Assertions.assertEquals(3, stateMap.size());

        Assertions.assertTrue(stateMap.containsKey(2, 1));
        Assertions.assertFalse(stateMap.containsKey(3, 1));
        Assertions.assertFalse(stateMap.containsKey(2, 3));
        stateMap.put(2, 1, null);
        Assertions.assertTrue(stateMap.containsKey(2, 1));
        Assertions.assertEquals(3, stateMap.size());
        Assertions.assertNull(stateMap.get(2, 1));
        stateMap.put(2, 1, state21);
        Assertions.assertEquals(3, stateMap.size());

        Assertions.assertEquals(state21, stateMap.removeAndGetOld(2, 1));
        Assertions.assertFalse(stateMap.containsKey(2, 1));
        Assertions.assertEquals(2, stateMap.size());

        stateMap.remove(1, 2);
        Assertions.assertFalse(stateMap.containsKey(1, 2));
        Assertions.assertEquals(1, stateMap.size());

        Assertions.assertNull(stateMap.removeAndGetOld(4, 2));
        Assertions.assertEquals(1, stateMap.size());

        StateTransformationFunction<ArrayList<Integer>, Integer> function =
                (previousState, value) -> {
                    previousState.add(value);
                    return previousState;
                };

        final int value = 4711;
        stateMap.transform(1, 1, value, function);
        state11 = function.apply(state11, value);
        Assertions.assertEquals(state11, stateMap.get(1, 1));
    }

    /** This test triggers incremental rehash and tests for corruptions. */
    @Test
    public void testIncrementalRehash() {
        final CopyOnWriteStateMap<Integer, Integer, ArrayList<Integer>> stateMap =
                new CopyOnWriteStateMap<>(new ArrayListSerializer<>(IntSerializer.INSTANCE));

        int insert = 0;
        int remove = 0;
        while (!stateMap.isRehashing()) {
            stateMap.put(insert++, 0, new ArrayList<>());
            if (insert % 8 == 0) {
                stateMap.remove(remove++, 0);
            }
        }
        Assertions.assertEquals(insert - remove, stateMap.size());
        while (stateMap.isRehashing()) {
            stateMap.put(insert++, 0, new ArrayList<>());
            if (insert % 8 == 0) {
                stateMap.remove(remove++, 0);
            }
        }
        Assertions.assertEquals(insert - remove, stateMap.size());

        for (int i = 0; i < insert; ++i) {
            if (i < remove) {
                Assertions.assertFalse(stateMap.containsKey(i, 0));
            } else {
                Assertions.assertTrue(stateMap.containsKey(i, 0));
            }
        }
    }

    /**
     * This test does some random modifications to a state map and a reference (hash map). Then
     * draws snapshots, performs more modifications and checks snapshot integrity.
     */
    @Test
    public void testRandomModificationsAndCopyOnWriteIsolation() throws Exception {
        final CopyOnWriteStateMap<Integer, Integer, ArrayList<Integer>> stateMap =
                new CopyOnWriteStateMap<>(new ArrayListSerializer<>(IntSerializer.INSTANCE));

        final HashMap<Tuple2<Integer, Integer>, ArrayList<Integer>> referenceMap = new HashMap<>();

        final Random random = new Random(42);

        // holds snapshots from the map under test
        CopyOnWriteStateMap.StateMapEntry<Integer, Integer, ArrayList<Integer>>[] snapshot = null;
        int snapshotSize = 0;

        // holds a reference snapshot from our reference map that we compare against
        Tuple3<Integer, Integer, ArrayList<Integer>>[] reference = null;

        int val = 0;

        int snapshotCounter = 0;
        int referencedSnapshotId = 0;

        final StateTransformationFunction<ArrayList<Integer>, Integer> transformationFunction =
                (previousState, value) -> {
                    if (previousState == null) {
                        previousState = new ArrayList<>();
                    }
                    previousState.add(value);
                    // we give back the original, attempting to spot errors in to copy-on-write
                    return previousState;
                };

        StateIncrementalVisitor<Integer, Integer, ArrayList<Integer>> updatingIterator =
                stateMap.getStateIncrementalVisitor(5);

        // the main loop for modifications
        for (int i = 0; i < 10_000_000; ++i) {

            int key = random.nextInt(20);
            int namespace = random.nextInt(4);
            Tuple2<Integer, Integer> compositeKey = new Tuple2<>(key, namespace);

            int op = random.nextInt(10);

            ArrayList<Integer> state = null;
            ArrayList<Integer> referenceState = null;

            switch (op) {
                case 0:
                case 1:
                    {
                        state = stateMap.get(key, namespace);
                        referenceState = referenceMap.get(compositeKey);
                        if (null == state) {
                            state = new ArrayList<>();
                            stateMap.put(key, namespace, state);
                            referenceState = new ArrayList<>();
                            referenceMap.put(compositeKey, referenceState);
                        }
                        break;
                    }
                case 2:
                    {
                        stateMap.put(key, namespace, new ArrayList<>());
                        referenceMap.put(compositeKey, new ArrayList<>());
                        break;
                    }
                case 3:
                    {
                        state = stateMap.putAndGetOld(key, namespace, new ArrayList<>());
                        referenceState = referenceMap.put(compositeKey, new ArrayList<>());
                        break;
                    }
                case 4:
                    {
                        stateMap.remove(key, namespace);
                        referenceMap.remove(compositeKey);
                        break;
                    }
                case 5:
                    {
                        state = stateMap.removeAndGetOld(key, namespace);
                        referenceState = referenceMap.remove(compositeKey);
                        break;
                    }
                case 6:
                    {
                        final int updateValue = random.nextInt(1000);
                        stateMap.transform(key, namespace, updateValue, transformationFunction);
                        referenceMap.put(
                                compositeKey,
                                transformationFunction.apply(
                                        referenceMap.remove(compositeKey), updateValue));
                        break;
                    }
                case 7:
                case 8:
                case 9:
                    if (!updatingIterator.hasNext()) {
                        updatingIterator = stateMap.getStateIncrementalVisitor(5);
                        if (!updatingIterator.hasNext()) {
                            break;
                        }
                    }
                    testStateIteratorWithUpdate(
                            updatingIterator, stateMap, referenceMap, op == 8, op == 9);
                    break;
                default:
                    {
                        Assertions.fail("Unknown op-code " + op);
                    }
            }

            Assertions.assertEquals(referenceMap.size(), stateMap.size());

            if (state != null) {
                Assertions.assertNotNull(referenceState);
                // mutate the states a bit...
                if (random.nextBoolean() && !state.isEmpty()) {
                    state.remove(state.size() - 1);
                    referenceState.remove(referenceState.size() - 1);
                } else {
                    state.add(val);
                    referenceState.add(val);
                    ++val;
                }
            }

            Assertions.assertEquals(referenceState, state);

            // snapshot triggering / comparison / release
            if (i > 0 && i % 500 == 0) {

                if (snapshot != null) {
                    // check our referenced snapshot
                    deepCheck(reference, convert(snapshot, snapshotSize));

                    if (i % 1_000 == 0) {
                        // draw and release some other snapshot while holding on the old snapshot
                        ++snapshotCounter;
                        stateMap.snapshotMapArrays();
                        stateMap.releaseSnapshot(snapshotCounter);
                    }

                    // release the snapshot after some time
                    if (i % 5_000 == 0) {
                        snapshot = null;
                        reference = null;
                        snapshotSize = 0;
                        stateMap.releaseSnapshot(referencedSnapshotId);
                    }

                } else {
                    // if there is no more referenced snapshot, we create one
                    ++snapshotCounter;
                    referencedSnapshotId = snapshotCounter;
                    snapshot = stateMap.snapshotMapArrays();
                    snapshotSize = stateMap.size();
                    reference = manualDeepDump(referenceMap);
                }
            }
        }
    }

    /**
     * Test operations specific for StateIncrementalVisitor in {@code
     * testRandomModificationsAndCopyOnWriteIsolation()}.
     *
     * <p>Check next, update and remove during global iteration of StateIncrementalVisitor.
     */
    private static void testStateIteratorWithUpdate(
            StateIncrementalVisitor<Integer, Integer, ArrayList<Integer>> updatingIterator,
            CopyOnWriteStateMap<Integer, Integer, ArrayList<Integer>> stateMap,
            HashMap<Tuple2<Integer, Integer>, ArrayList<Integer>> referenceMap,
            boolean update,
            boolean remove) {

        for (StateEntry<Integer, Integer, ArrayList<Integer>> stateEntry :
                updatingIterator.nextEntries()) {
            Integer key = stateEntry.getKey();
            Integer namespace = stateEntry.getNamespace();
            Tuple2<Integer, Integer> compositeKey = new Tuple2<>(key, namespace);
            Assertions.assertEquals(referenceMap.get(compositeKey), stateEntry.getState());

            if (update) {
                ArrayList<Integer> newState = new ArrayList<>(stateEntry.getState());
                if (!newState.isEmpty()) {
                    newState.remove(0);
                }
                updatingIterator.update(stateEntry, newState);
                referenceMap.put(compositeKey, new ArrayList<>(newState));
                Assertions.assertEquals(newState, stateMap.get(key, namespace));
            }

            if (remove) {
                updatingIterator.remove(stateEntry);
                referenceMap.remove(compositeKey);
            }
        }
    }

    /**
     * This tests for the copy-on-write contracts, e.g. ensures that no copy-on-write is active
     * after all snapshots are released.
     */
    @Test
    public void testCopyOnWriteContracts() {
        final CopyOnWriteStateMap<Integer, Integer, ArrayList<Integer>> stateMap =
                new CopyOnWriteStateMap<>(new ArrayListSerializer<>(IntSerializer.INSTANCE));

        ArrayList<Integer> originalState1 = new ArrayList<>(1);
        ArrayList<Integer> originalState2 = new ArrayList<>(1);
        ArrayList<Integer> originalState3 = new ArrayList<>(1);
        ArrayList<Integer> originalState4 = new ArrayList<>(1);
        ArrayList<Integer> originalState5 = new ArrayList<>(1);

        originalState1.add(1);
        originalState2.add(2);
        originalState3.add(3);
        originalState4.add(4);
        originalState5.add(5);

        stateMap.put(1, 1, originalState1);
        stateMap.put(2, 1, originalState2);
        stateMap.put(4, 1, originalState4);
        stateMap.put(5, 1, originalState5);

        // no snapshot taken, we get the original back
        Assertions.assertSame(stateMap.get(1, 1), originalState1);
        CopyOnWriteStateMapSnapshot<Integer, Integer, ArrayList<Integer>> snapshot1 =
                stateMap.stateSnapshot();
        // after snapshot1 is taken, we get a copy...
        final ArrayList<Integer> copyState = stateMap.get(1, 1);
        Assertions.assertNotSame(copyState, originalState1);
        // ...and the copy is equal
        Assertions.assertEquals(originalState1, copyState);

        // we make an insert AFTER snapshot1
        stateMap.put(3, 1, originalState3);

        // on repeated lookups, we get the same copy because no further snapshot was taken
        Assertions.assertSame(copyState, stateMap.get(1, 1));

        // we take snapshot2
        CopyOnWriteStateMapSnapshot<Integer, Integer, ArrayList<Integer>> snapshot2 =
                stateMap.stateSnapshot();
        // after the second snapshot, copy-on-write is active again for old entries
        Assertions.assertNotSame(copyState, stateMap.get(1, 1));
        // and equality still holds
        Assertions.assertEquals(copyState, stateMap.get(1, 1));

        // after releasing snapshot2
        stateMap.releaseSnapshot(snapshot2);
        // we still get the original of the untouched late insert (after snapshot1)
        Assertions.assertSame(originalState3, stateMap.get(3, 1));
        // but copy-on-write is still active for older inserts (before snapshot1)
        Assertions.assertNotSame(originalState4, stateMap.get(4, 1));

        // after releasing snapshot1
        stateMap.releaseSnapshot(snapshot1);
        // no copy-on-write is active
        Assertions.assertSame(originalState5, stateMap.get(5, 1));
    }

    /** This tests that snapshot can be released correctly. */
    @Test
    public void testSnapshotRelease() {
        final CopyOnWriteStateMap<Integer, Integer, Integer> stateMap =
                new CopyOnWriteStateMap<>(IntSerializer.INSTANCE);

        for (int i = 0; i < 10; i++) {
            stateMap.put(i, i, i);
        }

        CopyOnWriteStateMapSnapshot<Integer, Integer, Integer> snapshot = stateMap.stateSnapshot();
        Assertions.assertFalse(snapshot.isReleased());
        MatcherAssert.assertThat(
                stateMap.getSnapshotVersions(), Matchers.contains(snapshot.getSnapshotVersion()));

        snapshot.release();
        Assertions.assertTrue(snapshot.isReleased());
        MatcherAssert.assertThat(stateMap.getSnapshotVersions(), Matchers.empty());

        // verify that snapshot will release itself only once
        snapshot.release();
        MatcherAssert.assertThat(stateMap.getSnapshotVersions(), Matchers.empty());
    }

    @SuppressWarnings("unchecked")
    private static <K, N, S> Tuple3<K, N, S>[] convert(
            CopyOnWriteStateMap.StateMapEntry<K, N, S>[] snapshot, int mapSize) {

        Tuple3<K, N, S>[] result = new Tuple3[mapSize];
        int pos = 0;
        for (CopyOnWriteStateMap.StateMapEntry<K, N, S> entry : snapshot) {
            while (null != entry) {
                result[pos++] =
                        new Tuple3<>(entry.getKey(), entry.getNamespace(), entry.getState());
                entry = entry.next;
            }
        }
        Assertions.assertEquals(mapSize, pos);
        return result;
    }

    @SuppressWarnings("unchecked")
    private Tuple3<Integer, Integer, ArrayList<Integer>>[] manualDeepDump(
            HashMap<Tuple2<Integer, Integer>, ArrayList<Integer>> map) {

        Tuple3<Integer, Integer, ArrayList<Integer>>[] result = new Tuple3[map.size()];
        int pos = 0;
        for (Map.Entry<Tuple2<Integer, Integer>, ArrayList<Integer>> entry : map.entrySet()) {
            Integer key = entry.getKey().f0;
            Integer namespace = entry.getKey().f1;
            result[pos++] = new Tuple3<>(key, namespace, new ArrayList<>(entry.getValue()));
        }
        return result;
    }

    private void deepCheck(
            Tuple3<Integer, Integer, ArrayList<Integer>>[] a,
            Tuple3<Integer, Integer, ArrayList<Integer>>[] b) {

        if (a == b) {
            return;
        }

        Assertions.assertEquals(a.length, b.length);

        Comparator<Tuple3<Integer, Integer, ArrayList<Integer>>> comparator =
                (o1, o2) -> {
                    int namespaceDiff = o1.f1 - o2.f1;
                    return namespaceDiff != 0 ? namespaceDiff : o1.f0 - o2.f0;
                };

        Arrays.sort(a, comparator);
        Arrays.sort(b, comparator);

        for (int i = 0; i < a.length; ++i) {
            Tuple3<Integer, Integer, ArrayList<Integer>> av = a[i];
            Tuple3<Integer, Integer, ArrayList<Integer>> bv = b[i];

            Assertions.assertEquals(av.f0, bv.f0);
            Assertions.assertEquals(av.f1, bv.f1);
            Assertions.assertEquals(av.f2, bv.f2);
        }
    }
}
