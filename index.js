const AsyncIterableStream = require('async-iterable-stream');
const LinkedList = require('./linked-list');
const END_SYMBOL = Symbol('end');

class WritableAsyncIterableStream extends AsyncIterableStream {
  constructor(options) {
    super(() => {
      return this.createDataStream();
    });
    options = options || {};
    this.consumerTimeout = options.consumerTimeout || 30000;
    this.cleanupInterval = options.cleanupInterval || 10000;
    this.hasConsumer = false;
    this._nextConsumerId = 1;
    this._dataConsumers = {};
    this._dataLinkedList = new LinkedList();
    this._cleanupIntervalId = null;
  }

  _launchCleanupInterval() {
    if (this._cleanupIntervalId != null) {
      return;
    }
    this._cleanupIntervalId = setInterval(() => {
      let remainingConsumers = this.cleanupConsumers();
      if (remainingConsumers <= 0) {
        clearInterval(this._cleanupIntervalId);
        this._cleanupIntervalId = null;
      }
    }, this.cleanupInterval);
  }

  cleanupConsumers() {
    let deletedConsumerCount = 0;
    let dataConsumerIds = Object.keys(this._dataConsumers);
    dataConsumerIds.forEach((consumerId) => {
      let consumer = this._dataConsumers[consumerId];

      if (Date.now() - consumer.lastActivity >= this.consumerTimeout) {
        delete this._dataConsumers[consumerId];
        deletedConsumerCount++;
      }
    });

    let remainingConsumers = dataConsumerIds.length - deletedConsumerCount;

    if (remainingConsumers <= 0) {
      this.hasConsumer = false;
    }
    return remainingConsumers;
  }

  _cleanupBuffer(dataConsumerList) {
    let consumerStartNodes = new Set();
    dataConsumerList.forEach((consumer) => {
      consumerStartNodes.add(consumer.startNode);
    });

    let currentNode = this._dataLinkedList.head;
    let newFirstNode;
    let newFirstNodeIndex = -1;

    while (currentNode) {
      if (consumerStartNodes.has(currentNode)) {
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

  cleanupBuffer() {
    this._cleanupBuffer(Object.values(this._dataConsumers));
    return this._dataLinkedList.length;
  }

  write(data) {
    this._dataLinkedList.append(data);
    let dataConsumerList = Object.values(this._dataConsumers);
    dataConsumerList.forEach((consumer) => {
      let callback = consumer.callback;
      if (callback) {
        delete consumer.callback;
        callback();
      }
    });
    this._cleanupBuffer(dataConsumerList);
  }

  end() {
    this.write(END_SYMBOL);
    this._dataConsumers = {};
  }

  getBuffer() {
    let buffer = [];
    let currentNode = this._dataLinkedList.head.next;
    while (currentNode) {
      buffer.push(currentNode.value);
      currentNode = currentNode.next;
    }
    return buffer;
  }

  getConsumerDetails() {
    return Object.values(this._dataConsumers).map((consumer) => {
      return {
        id: consumer.id,
        lastActivity: consumer.lastActivity
      };
    });
  }

  async _waitForNextDataBuffer(consumerId) {
    return new Promise((resolve) => {
      let currentConsumer = this._dataConsumers[consumerId];
      let startNode = this._dataLinkedList.tail;
      this.hasConsumer = true;
      this._dataConsumers[consumerId] = {
        id: consumerId,
        startNode,
        lastActivity: Date.now(),
        callback: () => {
          resolve(startNode);
        }
      };
    });
  }

  async *createDataBufferStream() {
    let consumerId = this._nextConsumerId++;
    this._launchCleanupInterval();
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
