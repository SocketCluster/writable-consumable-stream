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

  _write(value, done) {
    let dataNode = {
      data: {value, done},
      next: null
    };
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

  getBackpressure(consumerId) {
    let consumer = this._consumers[consumerId];
    if (consumer) {
      return consumer.backpressure;
    }
    return 0;
  }

  getBackpressureList() {
    let backpressureList = [];
    let consumerList = Object.values(this._consumers);
    let len = consumerList.length;
    for (let i = 0; i < len; i++) {
      let consumer = consumerList[i];
      if (consumer.backpressure) {
        backpressureList.push({
          consumerId: consumer.id,
          backpressure: consumer.backpressure
        });
      }
    }
    return backpressureList;
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
        this._consumers[consumerId] = consumer;
        if (!currentNode.next) {
          try {
            await this._waitForNextDataNode(consumer, timeout);
          } catch (error) {
            delete this._consumers[consumerId];
            throw error;
          }
        }
        consumer.backpressure--;
        currentNode = currentNode.next;
        if (currentNode.data.done) {
          delete this._consumers[consumerId];
        }
        return currentNode.data;
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
