const AsyncIterableStream = require('async-iterable-stream');
const END_SYMBOL = Symbol('end');

class WritableAsyncIterableStream extends AsyncIterableStream {
  constructor() {
    super(() => {
      return this.createDataStream();
    });
    this._nextId = 1;
    // TODO: Cleanup inactive consumers
    this._dataConsumers = {};
  }

  write(data) {
    this._writeData(data);
  }

  end() {
    this.write(END_SYMBOL);
  }

  _writeData(data) {
    Object.values(this._dataConsumers).forEach((consumer) => {
      consumer.buffer.push(data);
      let callback = consumer.callback;
      if (callback) {
        delete consumer.callback;
        callback();
      }
    });
  }

  async waitForNextDataBuffer(consumerId) {
    return new Promise((resolve) => {
      let buffer = [];
      this._dataConsumers[consumerId] = {
        buffer,
        callback: () => {
          resolve(buffer);
        }
      };
    });
  }

  async *createDataBufferStream(consumerId) {
    while (true) {
      yield this.waitForNextDataBuffer(consumerId);
    }
  }

  async *createDataStream() {
    let consumerId = this._nextId++;
    let dataBufferStream = this.createDataBufferStream(consumerId);
    for await (let dataBuffer of dataBufferStream) {
      for (let data of dataBuffer) {
        if (data === END_SYMBOL) {
          return;
        }
        yield data;
      }
    }
  }
}

module.exports = WritableAsyncIterableStream;
