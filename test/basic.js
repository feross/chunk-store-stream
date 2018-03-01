var CacheChunkStore = require('cache-chunk-store')
var ChunkStoreStream = require('../')
var concat = require('simple-concat')
var FSChunkStore = require('fs-chunk-store')
var ImmediateChunkStore = require('immediate-chunk-store')
var MemoryChunkStore = require('memory-chunk-store')
var str = require('string-to-stream')
var test = require('tape')

runTests('FS', function (chunkLength) {
  return new FSChunkStore(chunkLength)
})

runTests('Memory', function (chunkLength) {
  return new MemoryChunkStore(chunkLength)
})

runTests('Cache(FS)', function (chunkLength) {
  return new CacheChunkStore(new FSChunkStore(chunkLength))
})

runTests('Cache(Memory)', function (chunkLength) {
  return new CacheChunkStore(new MemoryChunkStore(chunkLength))
})

runTests('Immediate(FS)', function (chunkLength) {
  return new ImmediateChunkStore(new FSChunkStore(chunkLength))
})

runTests('Immediate(Memory)', function (chunkLength) {
  return new ImmediateChunkStore(new MemoryChunkStore(chunkLength))
})

runTests('Cache(Immediate(FS)', function (chunkLength) {
  return new CacheChunkStore(new ImmediateChunkStore(new FSChunkStore(chunkLength)))
})

runTests('Cache(Immediate(Memory)', function (chunkLength) {
  return new CacheChunkStore(new ImmediateChunkStore(new MemoryChunkStore(chunkLength)))
})

function runTests (name, Store) {
  test(name + ': readable stream', function (t) {
    var store = new Store(3)
    store.put(0, Buffer.from('abc'), function (err) {
      t.error(err)
      store.put(1, Buffer.from('def'), function (err) {
        t.error(err)

        var stream = ChunkStoreStream.read(store, 3, { length: 6 })
        stream.on('error', function (err) { t.fail(err) })

        concat(stream, function (err, buf) {
          t.error(err)
          t.deepEqual(buf, Buffer.from('abcdef'))
          t.end()
        })
      })
    })
  })

  test(name + ': writable stream', function (t) {
    var store = new Store(3)

    var stream = ChunkStoreStream.write(store, 3)
    stream.on('error', function (err) { t.fail(err) })

    str('abcdef')
      .pipe(stream)
      .on('finish', function () {
        store.get(0, function (err, buf) {
          t.error(err)
          t.deepEqual(buf, Buffer.from('abc'))
          store.get(1, function (err, buf) {
            t.error(err)
            t.deepEqual(buf, Buffer.from('def'))
            t.end()
          })
        })
      })
  })
}
