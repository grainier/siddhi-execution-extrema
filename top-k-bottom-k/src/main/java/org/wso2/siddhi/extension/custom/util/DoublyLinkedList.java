/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.wso2.siddhi.extension.custom.util;

import java.util.ConcurrentModificationException;
import java.util.Iterator;

/**
 * A double linked list implementation
 *
 * @param <T> The type of values to be stored in the nodes of the list
 */
public class DoublyLinkedList<T> implements Iterable<T> {
    private int size;
    private ListNode<T> head;
    private ListNode<T> tail;

    /**
     * Add a node with value after the last node in the list
     *
     * @param value The value of the node to be added as the last node of the list
     */
    public ListNode<T> addAfterLast(T value) {
        ListNode<T> node = new ListNode<T>(value);
        addAfterLast(node);
        return node;
    }

    /**
     * Add a node after the last node in the list
     *
     * @param node The node to be added as the last node of the list
     */
    public void addAfterLast(ListNode<T> node) {
        node.setPreviousNode(tail);
        node.setNextNode(null);
        if (size == 0) {
            head = node;
        } else {
            tail.setNextNode(node);
        }
        tail = node;
        size++;
    }

    /**
     * Add a node with value before the first node in the list
     *
     * @param value The value of the node to be added as the first node of the list
     */
    public ListNode<T> addBeforeFirst(T value) {
        ListNode<T> node = new ListNode<T>(value);
        addBeforeFirst(node);
        return node;
    }

    /**
     * Add a node before the first node in the list
     *
     * @param node The node to be added as the first node of the list
     */
    public void addBeforeFirst(ListNode<T> node) {
        node.setNextNode(head);
        node.setPreviousNode(null);
        if (size == 0) {
            tail = node;
        } else {
            head.setPreviousNode(node);
        }
        head = node;
        size++;
    }

    public ListNode<T> addAfterNode(ListNode<T> node, T value) {
        ListNode<T> newNode = new ListNode<T>(value);
        addAfterNode(node, newNode);
        return newNode;
    }

    public void addAfterNode(ListNode<T> node, ListNode<T> newNode) {
        newNode.setNextNode(node.getNextNode());
        newNode.setPreviousNode(node);
        node.setNextNode(newNode);
        if (newNode.getNextNode() == null) {
            tail = newNode;
        } else {
            newNode.getNextNode().setPreviousNode(newNode);
        }
        size++;
    }

    public void remove(ListNode<T> node) {
        if (node == head) {
            head = node.getNextNode();
        } else {
            node.getPreviousNode().setNextNode(node.getNextNode());
        }
        if (node == tail) {
            tail = node.getPreviousNode();
        } else {
            node.getNextNode().setPreviousNode(node.getPreviousNode());
        }
        size--;
    }

    public int size() {
        return size;
    }

    public T first() {
        return head == null ? null : head.getValue();
    }

    public T last() {
        return tail == null ? null : tail.getValue();
    }

    public ListNode<T> head() {
        return head;
    }

    public ListNode<T> tail() {
        return tail;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public Iterator<T> iterator() {
        return new DoublyLinkedListIterator(this);
    }

    protected class DoublyLinkedListIterator implements Iterator<T> {
        private DoublyLinkedList<T> list;
        private ListNode<T> nextItrNode;
        private int length;

        public DoublyLinkedListIterator(DoublyLinkedList<T> list) {
            this.length = list.size;
            this.list = list;
            this.nextItrNode = list.head;
        }

        @Override
        public boolean hasNext() {
            return nextItrNode != null;
        }

        @Override
        public T next() {
            if (length != list.size) {
                throw new ConcurrentModificationException();
            }
            T next = nextItrNode.getValue();
            nextItrNode = nextItrNode.getNextNode();
            return next;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }
}
