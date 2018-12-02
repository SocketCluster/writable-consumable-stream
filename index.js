const AsyncIterableStream = require('async-iterable-stream');
const END_SYMBOL = Symbol('end');

class WritableAsyncIterableStream extends AsyncIterableStream {
  constructor() {
    super(() => {
      return this.createDataStream();
    });
    this._consumers = [];
    this._linkedListTailNode = {value: undefined, next: null, sentinel: true};
  }

  write(data) {
    let dataNode = {value: data, next: null};
    this._linkedListTailNode.next = dataNode;
    this._linkedListTailNode = dataNode;
    let len = this._consumers.length;
    for (let i = 0; i < len; i++) {
      this._consumers[i].callback();
    }
    this._consumers = [];
  }

  end() {
    this.write(END_SYMBOL);
  }

  async _waitForNextDataBuffer() {
    return new Promise((resolve) => {
      let startNode = this._linkedListTailNode;
      this._consumers.push({
        callback: () => {
          resolve(startNode);
        }
      });
    });
  }

  async *createDataBufferStream() {
    while (true) {
      yield this._waitForNextDataBuffer();
    }
  }

  async *createDataStream() {
    let dataBufferStream = this.createDataBufferStream();
    for await (let startNode of dataBufferStream) {
      let currentNode = startNode.next;
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
