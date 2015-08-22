module.exports = ChunkStoreStream

var BlockStream = require('block-stream2')
var inherits = require('inherits')
var stream = require('stream')

function ChunkStoreStream (store, chunkLength) {
  if (!store || !store.put || !store.get) {
    throw new Error('First argument must be an abstract-chunk-store compliant store')
  }
  chunkLength = Number(chunkLength)
  if (!chunkLength) throw new Error('Second argument must be a chunk length')

  return {
    createWriteStream: function (opts) {
      return new ChunkStoreWriteStream(store, chunkLength, opts)
    },
    createReadStream: function (opts) {
      return new ChunkStoreReadStream(store, chunkLength, opts)
    }
  }
}

inherits(ChunkStoreWriteStream, stream.Writable)

function ChunkStoreWriteStream (store, chunkLength, opts) {
  var self = this
  if (!(self instanceof ChunkStoreWriteStream)) {
    return new ChunkStoreWriteStream(store, chunkLength, opts)
  }
  stream.Writable.call(self, opts)
  if (!opts) opts = {}

  self._blockstream = new BlockStream(chunkLength, { zeroPadding: false })

  self._blockstream
    .on('data', onData)
    .on('error', function (err) { self.destroy(err) })

  var index = 0
  function onData (chunk) {
    if (self.destroyed) return
    store.put(index, chunk)
    index += 1
  }

  self.on('finish', function () { this._blockstream.end() })
}

ChunkStoreWriteStream.prototype._write = function (chunk, encoding, callback) {
  this._blockstream.write(chunk, encoding, callback)
}

ChunkStoreWriteStream.prototype.destroy = function (err) {
  if (this.destroyed) return
  this.destroyed = true

  if (err) this.emit('error', err)
  this.emit('close')
}

inherits(ChunkStoreReadStream, stream.Readable)

function ChunkStoreReadStream (store, chunkLength, opts) {
  if (!(this instanceof ChunkStoreReadStream)) {
    return new ChunkStoreReadStream(store, chunkLength, opts)
  }
  stream.Readable.call(this, opts)
  if (!opts) opts = {}

  this._store = store
  this._chunkLength = chunkLength
  this._index = 0
  this._length = opts.length || store.length
  if (!Number.isFinite(this._length)) throw new Error('missing required `length` property')
}

ChunkStoreReadStream.prototype._read = function () {
  var self = this
  if (self._index * self._chunkLength >= self._length) {
    self.push(null)
  } else {
    self._store.get(self._index, function (err, chunk) {
      if (err) return self.destroy(err)
      self.push(chunk)
    })
  }
  self._index += 1
}

ChunkStoreReadStream.prototype.destroy = function (err) {
  if (this.destroyed) return
  this.destroyed = true

  if (err) this.emit('error', err)
  this.emit('close')
}
