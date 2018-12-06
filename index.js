const AsyncIterableStream = require('async-iterable-stream');

class WritableAsyncIterableStream extends AsyncIterableStream {
  constructor() {
    super();
    this._consumers = [];
    this._linkedListTailNode = {next: null};
  }

  _write(value, done) {
    let dataNode = {
      data: {value, done},
      next: null
    };
    this._linkedListTailNode.next = dataNode;
    this._linkedListTailNode = dataNode;
    let len = this._consumers.length;
    for (let i = 0; i < len; i++) {
      this._consumers[i]();
    }
    this._consumers = [];
  }

  write(value) {
    this._write(value, false);
  }

  end() {
    this._write(undefined, true);
  }

  async _waitForNextDataNode() {
    return new Promise((resolve) => {
      this._consumers.push(() => {
        resolve();
      });
    });
  }

  createAsyncIterator() {
    let currentNode = this._linkedListTailNode;
    return {
      next: async () => {
        if (!currentNode.next) {
          await this._waitForNextDataNode();
        }
        currentNode = currentNode.next;
        return currentNode.data;
      }
    };
  }
}

module.exports = WritableAsyncIterableStream;
