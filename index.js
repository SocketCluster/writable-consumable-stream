const AsyncIterableStream = require('async-iterable-stream');
const LinkedList = require('./linked-list');
const END_SYMBOL = Symbol('end');

class WritableAsyncIterableStream extends AsyncIterableStream {
  constructor(options) {
    options = options || {};
    super(() => {
      return this.createDataStream();
    });
    this.consumerTimeout = options.consumerTimeout || 10000;
    this._nextId = 1;
    this._dataConsumers = {};
    this._dataLinkedList = new LinkedList();
  }

  write(data) {
    this._dataLinkedList.append(data);

    let allStartNodes = new Set();
    Object.keys(this._dataConsumers).forEach((consumerId) => {
      let consumer = this._dataConsumers[consumerId];
      allStartNodes.add(consumer.startNode);

      if (Date.now() - consumer.time >= this.consumerTimeout) {
        delete this._dataConsumers[consumerId];
        return;
      }

      let callback = consumer.callback;
      if (callback) {
        delete consumer.callback;
        callback();
      }
    });

    let currentNode = this._dataLinkedList.head;
    let newFirstNode;
    let newFirstNodeIndex = 0;
    while (currentNode) {
      if (allStartNodes.has(currentNode)) {
        newFirstNode = currentNode;
        break;
      }
      newFirstNodeIndex++;
      currentNode = currentNode.next;
    }

    if (newFirstNode) {
      if (!newFirstNode.sentinel) {
        this._dataLinkedList.head.next = newFirstNode;
        this._dataLinkedList.length -= newFirstNodeIndex;
      }
    } else {
      this._dataLinkedList.head.next = null;
      this._dataLinkedList.tail = this._dataLinkedList.head;
      this._dataLinkedList.length = 0;
    }
  }

  end() {
    this.write(END_SYMBOL);
    this._dataConsumers = {};
  }

  async _waitForNextDataBuffer(consumerId) {
    return new Promise((resolve) => {
      let currentConsumer = this._dataConsumers[consumerId];
      let startNode = this._dataLinkedList.tail;
      this._dataConsumers[consumerId] = {
        startNode,
        time: Date.now(),
        callback: () => {
          resolve(startNode);
        }
      };
    });
  }

  async *createDataBufferStream() {
    let consumerId = this._nextId++;
    while (true) {
      yield this._waitForNextDataBuffer(consumerId);
    }
  }

  async *createDataStream() {
    let dataBufferStream = this.createDataBufferStream();
    for await (let startNode of dataBufferStream) {
      let currentNode = startNode;
      currentNode = currentNode.next;
      while (currentNode) {
        let data = currentNode.value;
        if (data === END_SYMBOL) {
          return;
        }
        yield data;
        currentNode = currentNode.next;
      }
    }
  }
}

module.exports = WritableAsyncIterableStream;
