const ConsumableStream = require('consumable-stream');
const Consumer = require('./consumer');

class WritableConsumableStream extends ConsumableStream {
  constructor() {
    super();
    this.nextConsumerId = 1;
    this._consumers = {};

    // Tail node of a singly linked list.
    this._tailNode = {
      next: null,
      data: {
        value: undefined,
        done: false
      }
    };
  }

  _write(value, done, consumerId) {
    let dataNode = {
      data: {value, done},
      next: null
    };
    if (consumerId) {
      dataNode.consumerId = consumerId;
    }
    this._tailNode.next = dataNode;
    this._tailNode = dataNode;

    let consumerList = Object.values(this._consumers);
    let len = consumerList.length;

    for (let i = 0; i < len; i++) {
      let consumer = consumerList[i];
      consumer.write(dataNode.data);
    }
  }

  write(value) {
    this._write(value, false);
  }

  close(value) {
    this._write(value, true);
  }

  writeToConsumer(consumerId, value) {
    this._write(value, false, consumerId);
  }

  closeConsumer(consumerId, value) {
    this._write(value, true, consumerId);
  }

  kill(value) {
    let consumerIdList = Object.keys(this._consumers);
    let len = consumerIdList.length;
    for (let i = 0; i < len; i++) {
      this.killConsumer(consumerIdList[i], value);
    }
  }

  killConsumer(consumerId, value) {
    let consumer = this._consumers[consumerId];
    if (!consumer) {
      return;
    }
    consumer.kill(value);
  }

  getBackpressure() {
    let consumerList = Object.values(this._consumers);
    let len = consumerList.length;

    let maxBackpressure = 0;
    for (let i = 0; i < len; i++) {
      let consumer = consumerList[i];
      let backpressure = consumer.getBackpressure();
      if (backpressure > maxBackpressure) {
        maxBackpressure = backpressure;
      }
    }
    return maxBackpressure;
  }

  getConsumerBackpressure(consumerId) {
    let consumer = this._consumers[consumerId];
    if (consumer) {
      return consumer.getBackpressure();
    }
    return 0;
  }

  hasConsumer(consumerId) {
    return !!this._consumers[consumerId];
  }

  setConsumer(consumerId, consumer) {
    this._consumers[consumerId] = consumer;
    if (!consumer.currentNode) {
      consumer.currentNode = this._tailNode;
    }
  }

  removeConsumer(consumerId) {
    delete this._consumers[consumerId];
  }

  getConsumerStats(consumerId) {
    let consumer = this._consumers[consumerId];
    if (consumer) {
      return consumer.getStats();
    }
    return undefined;
  }

  getConsumerStatsList() {
    let consumerStats = [];
    let consumerList = Object.values(this._consumers);
    let len = consumerList.length;
    for (let i = 0; i < len; i++) {
      let consumer = consumerList[i];
      consumerStats.push(consumer.getStats());
    }
    return consumerStats;
  }

  createConsumer(timeout) {
    return new Consumer(this, this.nextConsumerId++, this._tailNode, timeout);
  }
}

module.exports = WritableConsumableStream;
