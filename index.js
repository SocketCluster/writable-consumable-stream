const AsyncIterableStream = require('async-iterable-stream');

class WritableAsyncIterableStream extends AsyncIterableStream {
  constructor() {
    super();
    this._nextConsumerId = 1;
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
      throw new Error('The specified consumer does not exist');
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

  getMaxBackpressure() {
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

  getBackpressure(consumerId) {
    let consumer = this._consumers[consumerId];
    if (consumer) {
      return consumer.backpressure;
    }
    return 0;
  }

  getNextConsumerId() {
    return this._nextConsumerId;
  }

  hasConsumer(consumerId) {
    return !!this._consumers[consumerId];
  }

  getConsumerStats() {
    let consumerStats = [];
    let consumerList = Object.values(this._consumers);
    let len = consumerList.length;
    for (let i = 0; i < len; i++) {
      let consumer = consumerList[i];
      consumerStats.push({
        id: consumer.id,
        backpressure: consumer.backpressure
      });
    }
    return consumerStats;
  }

  async _waitForNextDataNode(consumer, timeout) {
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

  createAsyncIterator(timeout) {
    let consumerId = this._nextConsumerId++;
    let currentNode = this._linkedListTailNode;
    let consumer = {
      id: consumerId,
      backpressure: 0
    };
    this._consumers[consumerId] = consumer;

    return {
      consumerId,
      next: async () => {
        while (true) {
          this._consumers[consumerId] = consumer;
          if (!currentNode.next) {
            try {
              await this._waitForNextDataNode(consumer, timeout);
            } catch (error) {
              delete this._consumers[consumerId];
              throw error;
            }
          }
          if (consumer.kill) {
            consumer.backpressure = 0;
            delete this._consumers[consumerId];
            let killPacket = consumer.kill;
            delete consumer.kill;
            return killPacket;
          }

          consumer.backpressure--;
          currentNode = currentNode.next;

          if (currentNode.consumerId && currentNode.consumerId !== consumerId) {
            continue;
          }

          if (currentNode.data.done) {
            delete this._consumers[consumerId];
          }
          return currentNode.data;
        }
      },
      return: () => {
        delete this._consumers[consumerId];
        return {};
      }
    };
  }
}

function wait(timeout) {
  let timeoutId;
  let promise = new Promise((resolve) => {
    timeoutId = setTimeout(resolve, timeout);
  });
  return {timeoutId, promise};
}

module.exports = WritableAsyncIterableStream;
