const CacheChunkStore = require('cache-chunk-store')
const concat = require('simple-concat')
const FSChunkStore = require('fs-chunk-store')
const ImmediateChunkStore = require('immediate-chunk-store')
const MemoryChunkStore = require('memory-chunk-store')
const str = require('string-to-stream')
const test = require('tape')
const { ChunkStoreReadStream, ChunkStoreWriteStream } = require('../')

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
  test(`${name}: readable stream`, t => {
    const store = new Store(3)
    store.put(0, Buffer.from('abc'), err => {
      t.error(err)
      store.put(1, Buffer.from('def'), err => {
        t.error(err)

        const stream = new ChunkStoreReadStream(store, 3, { length: 6 })
        stream.on('error', err => { t.fail(err) })

        concat(stream, (err, buf) => {
          t.error(err)
          t.deepEqual(buf, Buffer.from('abcdef'))
          t.end()
        })
      })
    })
  })

  test(`${name}: writable stream`, t => {
    const store = new Store(3)

    const stream = new ChunkStoreWriteStream(store, 3)
    stream.on('error', err => { t.fail(err) })

    str('abcdef')
      .pipe(stream)
      .on('finish', () => {
        store.get(0, (err, buf) => {
          t.error(err)
          t.deepEqual(buf, Buffer.from('abc'))
          store.get(1, (err, buf) => {
            t.error(err)
            t.deepEqual(buf, Buffer.from('def'))
            t.end()
          })
        })
      })
  })
}
