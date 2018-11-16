# iterable-async-stream
An async stream which can be iterated over using a for-await-of loop.

## Usage

### Consume stream and write to it asynchronously:

```js
let iterableStream = new IterableAsyncStream();

async function consumeAsyncIterable(iterable) {
  // Consume iterable data asynchronously.
  for await (let packet of iterable) {
    console.log('Packet:', packet);
  }
}
consumeAsyncIterable(iterableStream);

setInterval(() => {
  // Write data to the stream asynchronously,
  iterableStream.write(`Timestamp: ${Date.now()}`);
}, 100);
```

### Consume filtered stream using an async generator:

```js
let iterableStream = new IterableAsyncStream();

// Creates an async generator which only produces packets which are allowed by the
// specified filterFunction.
async function* createFilteredStreamGenerator(fullStream, filterFunction) {
  for await (let packet of fullStream) {
    if (filterFunction(packet)) {
      yield packet;
    }
  }
};

async function consumeAsyncIterable(iterable) {
  // Consume iterable data asynchronously.
  for await (let packet of iterable) {
    console.log('Packet:', packet);
  }
}

// The filter function will only include strings which end with the number 5.
function filterFn(data) {
  return /5$/.test(data);
}
let filteredStreamGenerator = createFilteredStreamGenerator(iterableStream, filterFn);

consumeAsyncIterable(filteredStreamGenerator);

setInterval(() => {
  // Write data to the stream asynchronously,
  iterableStream.write(`Timestamp: ${Date.now()}`);
}, 100);
```


See `test/` directory for additional examples.
