const ConsumableStream = require('consumable-stream');
const Consumer = require('./consumer');

class WritableConsumableStream extends ConsumableStream {
  constructor(options) {
    super();
    options = options || {};
    this._nextConsumerId = 1;
    this.generateConsumerId = options.generateConsumerId;
    if (!this.generateConsumerId) {
      this.generateConsumerId = () => this._nextConsumerId++;
    }
    this.removeConsumerCallback = options.removeConsumerCallback;
    this._consumers = new Map();

    // Tail node of a singly linked list.
    this.tailNode = {
      next: null,
      seq: 0,
      data: {
        value: undefined,
        done: false
      }
    };
  }

  _write(value, done, consumerId) {
    let dataNode = {
      data: {value, done},
      next: null,
      seq: this.tailNode.seq + 1
    };
    if (consumerId !== undefined) {
      dataNode.consumerId = consumerId;
    }
    this.tailNode.next = dataNode;
    this.tailNode = dataNode;

    for (let consumer of this._consumers.values()) {
      if (dataNode.consumerId !== undefined && dataNode.consumerId !== consumer.id) {
        consumer.skipNode(dataNode);
      } else {
        consumer.write(dataNode.data);
      }
    }
  }

  write(value) {
    this._write(value, false);
  }

  close(value) {
    this._write(value, true);
  }

  writeToConsumer(consumerId, value) {
    if (consumerId == null) {
      throw new TypeError('Cannot write to a consumer without a valid consumer id');
    }
    this._write(value, false, consumerId);
  }

  closeConsumer(consumerId, value) {
    if (consumerId == null) {
      throw new TypeError('Cannot close a consumer without a valid consumer id');
    }
    this._write(value, true, consumerId);
  }

  kill(value) {
    for (let consumerId of this._consumers.keys()) {
      this.killConsumer(consumerId, value);
    }
  }

  killConsumer(consumerId, value) {
    let consumer = this._consumers.get(consumerId);
    if (!consumer) {
      return;
    }
    consumer.kill(value);
  }

  // The backpressure of the stream as a whole is the size of its queue; the
  // main use case is memory management, so what matters is how many nodes are
  // being pinned, not how many of them any single consumer still owes.
  getBackpressure() {
    return this.getQueueDepth();
  }

  // How many nodes of the shared queue the furthest-behind consumer is still
  // pinning in memory, whether or not those nodes are addressed to it. This
  // is what stream.getBackpressure() reports. Consumer-level backpressure is
  // a different measure: how much work that consumer still owes.
  getQueueDepth() {
    let maxDepth = 0;
    for (let consumer of this._consumers.values()) {
      let depth = consumer.getQueueDepth();
      if (depth > maxDepth) {
        maxDepth = depth;
      }
    }
    return maxDepth;
  }

  getConsumerQueueDepth(consumerId) {
    let consumer = this._consumers.get(consumerId);
    if (consumer) {
      return consumer.getQueueDepth();
    }
    return 0;
  }

  getConsumerBackpressure(consumerId) {
    let consumer = this._consumers.get(consumerId);
    if (consumer) {
      return consumer.getBackpressure();
    }
    return 0;
  }

  hasConsumer(consumerId) {
    return this._consumers.has(consumerId);
  }

  setConsumer(consumerId, consumer) {
    this._consumers.set(consumerId, consumer);
    consumer.isAlive = true;
    if (!consumer.currentNode) {
      consumer.currentNode = this.tailNode;
    }
  }

  removeConsumer(consumerId) {
    let result = this._consumers.delete(consumerId);
    if (result && this.removeConsumerCallback) this.removeConsumerCallback(consumerId);
    return result;
  }

  getConsumerStats(consumerId) {
    let consumer = this._consumers.get(consumerId);
    if (consumer) {
      return consumer.getStats();
    }
    return undefined;
  }

  getConsumerStatsList() {
    let consumerStats = [];
    for (let consumer of this._consumers.values()) {
      consumerStats.push(consumer.getStats());
    }
    return consumerStats;
  }

  createConsumer(timeout) {
    return new Consumer(this, this.generateConsumerId(), this.tailNode, timeout);
  }

  getConsumerList() {
    return [...this._consumers.values()];
  }

  getConsumerCount() {
    return this._consumers.size;
  }
}

module.exports = WritableConsumableStream;
