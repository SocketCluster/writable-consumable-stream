const ConsumableStream = require('consumable-stream');
const Consumer = require('./consumer');

class WritableConsumableStream extends ConsumableStream {
  constructor() {
    super();
    this.nextConsumerId = 1;
    this._consumers = {};
    this._linkedListTailNode = {
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
    this._linkedListTailNode.next = dataNode;
    this._linkedListTailNode = dataNode;

    let consumerList = Object.values(this._consumers);
    let len = consumerList.length;

    for (let i = 0; i < len; i++) {
      let consumer = consumerList[i];
      if (consumer.timeoutId !== undefined) {
        clearTimeout(consumer.timeoutId);
        delete consumer.timeoutId;
      }
      consumer.backpressure++;

      if (consumer.resolve) {
        consumer.resolve();
        delete consumer.resolve;
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
    if (consumer.timeoutId !== undefined) {
      clearTimeout(consumer.timeoutId);
      delete consumer.timeoutId;
    }
    consumer.backpressure++;
    consumer.kill = {value, done: true};
    if (consumer.resolve) {
      consumer.resolve();
      delete consumer.resolve;
    }
  }

  getBackpressure() {
    let consumerList = Object.values(this._consumers);
    let len = consumerList.length;

    let maxBackpressure = 0;
    for (let i = 0; i < len; i++) {
      let consumer = consumerList[i];
      if (consumer.backpressure > maxBackpressure) {
        maxBackpressure = consumer.backpressure;
      }
    }
    return maxBackpressure;
  }

  getConsumerBackpressure(consumerId) {
    let consumer = this._consumers[consumerId];
    if (consumer) {
      return consumer.backpressure;
    }
    return 0;
  }

  hasConsumer(consumerId) {
    return !!this._consumers[consumerId];
  }

  setConsumer(consumerId, consumer) {
    this._consumers[consumerId] = consumer;
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

  getAllConsumerStats() {
    let consumerStats = [];
    let consumerList = Object.values(this._consumers);
    let len = consumerList.length;
    for (let i = 0; i < len; i++) {
      let consumer = consumerList[i];
      consumerStats.push(consumer.getStats());
    }
    return consumerStats;
  }

  async waitForNextItem(consumer, timeout) {
    return new Promise((resolve, reject) => {
      let timeoutId;
      if (timeout !== undefined) {
        // Create the error object in the outer scope in order
        // to get the full stack trace.
        let error = new Error('Stream consumer iteration timed out');
        (async () => {
          let delay = wait(timeout);
          timeoutId = delay.timeoutId;
          await delay.promise;
          error.name = 'TimeoutError';
          delete consumer.resolve;
          reject(error);
        })();
      }
      consumer.resolve = resolve;
      consumer.timeoutId = timeoutId;
    });
  }

  createConsumer(timeout) {
    return new Consumer(this, this.nextConsumerId++, this._linkedListTailNode, timeout);
  }
}

function wait(timeout) {
  let timeoutId;
  let promise = new Promise((resolve) => {
    timeoutId = setTimeout(resolve, timeout);
  });
  return {timeoutId, promise};
}

module.exports = WritableConsumableStream;
