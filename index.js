const AsyncIterableStream = require('async-iterable-stream');
const END_SYMBOL = Symbol('end');

class WritableAsyncIterableStream extends AsyncIterableStream {
  constructor() {
    super(() => {
      return this.createDataStream();
    });
    this._consumers = [];
    this._linkedListTailNode = {
      value: undefined,
      next: null,
      sentinel: true
    };
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

  async _waitForNextNodePointer() {
    return new Promise((resolve) => {
      let startNode = this._linkedListTailNode;
      this._consumers.push({
        callback: () => {
          resolve(startNode);
        }
      });
    });
  }

  async *_createNodePointerStream() {
    while (true) {
      yield this._waitForNextNodePointer();
    }
  }

  async *createDataStream() {
    let dataPointerStream = this._createNodePointerStream();
    for await (let startNode of dataPointerStream) {
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
