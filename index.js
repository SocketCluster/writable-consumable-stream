const AsyncIterableStream = require('async-iterable-stream');
const END_SYMBOL = Symbol('end');

class WritableAsyncIterableStream extends AsyncIterableStream {
  constructor() {
    super(() => {
      return this.createDataStream();
    });
    this.waitForNextDataBuffer();
  }

  write(data) {
    this._writeData(data);
  }

  end() {
    this.write(END_SYMBOL);
  }

  async waitForNextDataBuffer() {
    if (this._pendingPromise) {
      return this._pendingPromise;
    }
    this._pendingPromise = new Promise((resolve) => {
      let dataBuffer = [];
      this._writeData = (data) => {
        dataBuffer.push(data);
        if (dataBuffer.length === 1) {
          resolve(dataBuffer);
        }
      };
    });
    let buffer = await this._pendingPromise;
    delete this._pendingPromise;
    return buffer;
  }

  async *createDataBufferStream() {
    while (true) {
      yield this.waitForNextDataBuffer();
    }
  }

  async *createDataStream() {
    let dataBufferStream = this.createDataBufferStream();
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
