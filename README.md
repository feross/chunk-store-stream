# chunk-store-stream [![travis][travis-image]][travis-url] [![npm][npm-image]][npm-url] [![downloads][downloads-image]][downloads-url] [![javascript style guide][standard-image]][standard-url]

[![Greenkeeper badge](https://badges.greenkeeper.io/feross/chunk-store-stream.svg)](https://greenkeeper.io/)

[travis-image]: https://img.shields.io/travis/feross/chunk-store-stream/master.svg
[travis-url]: https://travis-ci.org/feross/chunk-store-stream
[npm-image]: https://img.shields.io/npm/v/chunk-store-stream.svg
[npm-url]: https://npmjs.org/package/chunk-store-stream
[downloads-image]: https://img.shields.io/npm/dm/chunk-store-stream.svg
[downloads-url]: https://npmjs.org/package/chunk-store-stream
[standard-image]: https://img.shields.io/badge/code_style-standard-brightgreen.svg
[standard-url]: https://standardjs.com

#### Convert an [abstract-chunk-store](https://github.com/mafintosh/abstract-chunk-store) store into a readable or writable stream

[![abstract chunk store](https://cdn.rawgit.com/mafintosh/abstract-chunk-store/master/badge.svg)](https://github.com/mafintosh/abstract-chunk-store)

Read/write data from/to a chunk store, with streams.

## Install

```
npm install chunk-store-stream
```

## Usage

### Create a read stream

``` js
var ChunkStoreStream = require('chunk-store-stream')
var FSChunkStore = require('fs-chunk-store') // any chunk store will work

var chunkLength = 3
var store = new FSChunkStore(chunkLength)

// ... put some data in the store

var stream = new ChunkStoreStream.read(store, chunkLength, { length: 6 })
stream.pipe(process.stdout)
```

### Create a write stream

```js
var ChunkStoreStream = require('chunk-store-stream')
var FSChunkStore = require('fs-chunk-store') // any chunk store will work
var fs = require('fs')

var chunkLength = 3
var store = new FSChunkStore(chunkLength)

var stream = new ChunkStoreStream.write(store, chunkLength)
fs.createReadStream('file.txt').pipe(stream)
```

## License

MIT. Copyright (c) [Feross Aboukhadijeh](http://feross.org).
