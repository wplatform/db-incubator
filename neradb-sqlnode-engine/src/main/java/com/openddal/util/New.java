/*
 * Copyright 2014-2016 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the “License”);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an “AS IS” BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.openddal.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;

import com.openddal.value.CaseInsensitiveMap;

/**
 * This class contains static methods to construct commonly used generic objects
 * such as ArrayList.
 */
public class New {

    /**
     * Create a new ArrayList.
     *
     * @param <T> the type
     * @return the object
     */
    public static <T> ArrayList<T> arrayList() {
        return new ArrayList<T>(4);
    }

    /**
     * Create a new HashMap.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @return the object
     */
    public static <K, V> HashMap<K, V> hashMap() {
        return new HashMap<K, V>();
    }

    /**
     * Create a new HashSet.
     *
     * @param <T> the type
     * @return the object
     */
    public static <T> HashSet<T> hashSet() {
        return new HashSet<T>();
    }

    /**
     * Create a new ArrayList.
     *
     * @param <T> the type
     * @param c the collection
     * @return the object
     */
    public static <T> ArrayList<T> arrayList(Collection<T> c) {
        return new ArrayList<T>(c);
    }

    /**
     * Create a new ArrayList.
     *
     * @param <T> the type
     * @param initialCapacity the initial capacity
     * @return the object
     */
    public static <T> ArrayList<T> arrayList(int initialCapacity) {
        return new ArrayList<T>(initialCapacity);
    }

    /**
     * Create a new ArrayList.
     *
     * @param <T> the type
     * @return the object
     */
    public static <T> LinkedList<T> linkedList() {
        return new LinkedList<T>();
    }

    /**
     * Create a new CopyOnWriteArrayList.
     *
     * @param <T> the type
     * @param initialCapacity the initial capacity
     * @return the object
     */
    public static <T> CopyOnWriteArrayList<T> copyOnWriteArrayList() {
        return new CopyOnWriteArrayList<T>();
    }

    /**
     * Create a new HashMap.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param initialCapacity the initial capacity
     * @return the object
     */
    public static <K, V> HashMap<K, V> hashMap(int initialCapacity) {
        return new HashMap<K, V>(initialCapacity);
    }

    /**
     * Create a new HashMap.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param initialCapacity the initial capacity
     * @return the object
     */
    public static <K, V> HashMap<K, V> hashMapNonRehash(int initialCapacity) {
        int size = (int) (initialCapacity / 0.75) + 1;
        return new HashMap<K, V>(size);
    }

    /**
     * Create a new HashMap.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param initialCapacity the initial capacity
     * @return the object
     */
    public static <K, V> HashMap<K, V> hashMap(int initialCapacity, float loadFactor) {
        return new HashMap<K, V>(initialCapacity, loadFactor);
    }

    /**
     * Create a new LinkedHashMap.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param initialCapacity the initial capacity
     * @return the object
     */
    public static <K, V> LinkedHashMap<K, V> linkedHashMap() {
        return new LinkedHashMap<K, V>();
    }

    /**
     * Create a new LinkedHashMap.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param initialCapacity the initial capacity
     * @return the object
     */
    public static <K, V> LinkedHashMap<K, V> linkedHashMap(int initialCapacity) {
        return new LinkedHashMap<K, V>(initialCapacity);
    }

    /**
     * Create a new LinkedHashMap.
     *
     * @param initialCapacity
     * @param loadFactor
     * @return
     */
    public static <K, V> LinkedHashMap<K, V> linkedHashMap(int initialCapacity, float loadFactor) {
        return new LinkedHashMap<K, V>(initialCapacity);
    }

    /**
     * Create a new ConcurrentHashMap.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param initialCapacity the initial capacity
     * @return the object
     */
    public static <K, V> ConcurrentHashMap<K, V> concurrentHashMap() {
        return new ConcurrentHashMap<K, V>();
    }

    /**
     * Create a new ConcurrentHashMap.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param initialCapacity the initial capacity
     * @return the object
     */
    public static <K, V> ConcurrentHashMap<K, V> concurrentHashMap(int initialCapacity) {
        return new ConcurrentHashMap<K, V>(initialCapacity);
    }

    /**
     * Create a new ConcurrentHashMap.
     *
     * @param initialCapacity
     * @param loadFactor
     * @return
     */
    public static <K, V> ConcurrentHashMap<K, V> concurrentHashMap(int initialCapacity, float loadFactor) {
        return new ConcurrentHashMap<K, V>(initialCapacity);
    }

    /**
     * Create a new CaseInsensitiveMap.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param initialCapacity the initial capacity
     * @return the object
     */
    public static <K, V> CaseInsensitiveMap<V> caseInsensitiveMap() {
        return new CaseInsensitiveMap<V>();
    }

    /**
     * Create a new HashSet.
     *
     * @param initialCapacity
     * @return
     */
    public static <T> HashSet<T> hashSet(int initialCapacity) {
        return new HashSet<T>(initialCapacity);
    }

    /**
     * Create a new HashSet.
     *
     * @param initialCapacity
     * @param loadFactor
     * @return
     */
    public static <T> HashSet<T> hashSet(int initialCapacity, float loadFactor) {
        return new HashSet<T>(initialCapacity, loadFactor);
    }

    /**
     * Create a new LinkedHashSet.
     *
     * @param <T> the type
     * @return the object
     */
    public static <T> HashSet<T> linkedHashSet() {
        return new LinkedHashSet<T>();
    }

    /**
     * Create a new CopyOnWriteArraySet.
     *
     * @param <T> the type
     * @return the object
     */
    public static <T> CopyOnWriteArraySet<T> copyOnWriteArraySet() {
        return new CopyOnWriteArraySet<T>();
    }

}
