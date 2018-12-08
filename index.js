const AsyncIterableStream = require('async-iterable-stream');

class WritableAsyncIterableStream extends AsyncIterableStream {
  constructor() {
    super();
    this._nextConsumerId = 1;
    this._consumers = {};
    this._linkedListTailNode = {next: null};
  }

  _write(value, done) {
    let dataNode = {
      data: {value, done},
      next: null
    };
    this._linkedListTailNode.next = dataNode;
    this._linkedListTailNode = dataNode;
    Object.values(this._consumers).forEach((consumer) => {
      if (consumer.timeoutId !== undefined) {
        clearTimeout(consumer.timeoutId);
      }
      consumer.resolve();
    });
    this._consumers = {};
    this._nextConsumerId = 1;
  }

  write(value) {
    this._write(value, false);
  }

  close() {
    this._write(undefined, true);
  }

  async _waitForNextDataNode(timeout) {
    return new Promise((resolve, reject) => {
      let timeoutId;
      let consumerId = this._nextConsumerId++;
      if (timeout !== undefined) {
        // Create the error object in the outer scope in order
        // to get the full stack trace.
        let error = new Error('Stream consumer iteration timed out');
        (async () => {
          let delay = wait(timeout);
          timeoutId = delay.timeoutId;
          await delay.promise;
          error.name = 'TimeoutError';
          delete this._consumers[consumerId];
          reject(error);
        })();
      }
      this._consumers[consumerId] = {
        resolve,
        timeoutId
      };
    });
  }

  createAsyncIterator(timeout) {
    let currentNode = this._linkedListTailNode;
    return {
      next: async () => {
        if (!currentNode.next) {
          await this._waitForNextDataNode(timeout);
        }
        currentNode = currentNode.next;
        return currentNode.data;
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
