'use strict'

const net = require('net')
const os = require('os')
const fs = require('fs')
const path = require('path')
const EventEmitter = require('events').EventEmitter
const assert = require('assert')
const debug = require('debug')(require('../package.json').name)

const { isError, MPDError } = require('./error')
const {
  isString,
  isNonEmptyString,
  escapeArg,
  parseList,
  parseNestedList,
  parseListAndAccumulate,
  parseObject,
  normalizeKeys,
  autoparseValues
} = require('./parsers')

const { Command } = require('./command')

const MPD_SENTINEL = /^(OK|ACK|list_OK)(.*)$/m
const OK_MPD = /^OK MPD /

class MPDClient extends EventEmitter {
  constructor (config) {
    super()
    this._config = config
    this._promiseQueue = []
    this._buf = ''
    this._msgParts = []
    this._idleevts = {}
    this._disconnecting = false

    // bind to this client
    this.disconnect = this.disconnect.bind(this)
    this._receive = this._receive.bind(this)
    this._handleIdling = this._handleIdling.bind(this)
    this._triggerIdleEvents = this._triggerIdleEvents.bind(this)
  }

  static connect (config) {
    if (!config || typeof config !== 'object') {
      config = getDefaultConfig()
      debug('connect: using config %o', config)
    }

    // allow tilde shortcuts if connecting to a socket
    if (isString(config.path) && config.path.startsWith('~')) {
      config.path = config.path.replace(/^~/, os.homedir())
    }

    return finalizeClientConnection(
      new MPDClient(config), net.connect(config))
  }

  async sendCommand (command) {
    assert.ok(this.idling)
    const promise = this._enqueuePromise()
    this.stopIdling()
    this.send(command)
    this.setupIdling()
    return promise
  }

  async sendCommands (commandList) {
    const cmd = 'command_list_begin\n' +
      commandList.join('\n') +
      '\ncommand_list_end'
    return this.sendCommand(cmd)
  }

  stopIdling () {
    if (!this.idling) {
      return
    }
    this.idling = false
    this.send('noidle')
  }

  setupIdling () {
    if (this.idling) {
      debug('already idling')
      return
    }
    if (this._disconnecting) {
      debug('client is being disconnected, ignoring idling setup')
      return
    }
    this.idling = true
    this._enqueuePromise().then(this._handleIdling)
    this.send('idle')
  }

  send (data) {
    if (!this.socket.writable) {
      throw new MPDError('Not connected', 'ENOTCONNECTED')
    }
    debug('sending %s', data)
    this.socket.write(data + '\n')
  }

  disconnect () {
    this._disconnecting = true
    return new Promise((resolve) => {
      if (this.socket && this.socket.destroyed) {
        return resolve()
      }

      let _resolve = () => {
        if (resolve) {
          resolve()
          resolve = null
        }
      }

      this.socket.once('close', _resolve)
      this.socket.once('end', _resolve)

      this.socket.end()
      setTimeout(() => this.socket.destroy(), 32)
    })
  }

  _enqueuePromise () {
    return new Promise((resolve, reject) =>
      this._promiseQueue.push({ resolve, reject }))
  }

  _resolve (msg) { this._promiseQueue.shift().resolve(msg) }
  _reject (err) { this._promiseQueue.shift().reject(err) }

  // complete lines of the pending response are kept in _msgParts
  // and joined once it ends; _buf holds an unfinished line or a binary
  // header and its incomplete payload (including the trailing newline)
  _receive (data) {
    let matched
    const buf = this._buf + data
    let msgStart = 0
    let lineStart = 0
    let endPos

    while ((endPos = buf.indexOf('\n', lineStart)) > -1) {
      const line = buf.substring(lineStart, endPos)

      if ((matched = line.match(MPD_SENTINEL)) !== null) {
        const code = matched[1]
        const desc = matched[2]

        if (lineStart > msgStart) {
          this._msgParts.push(buf.substring(msgStart, lineStart))
        }
        const msg = this._msgParts.join('')
        this._msgParts = []

        code !== 'ACK'
          ? this._resolve(msg || code) // if empty msg, send back OK
          : this._reject(new MPDError(desc))

        msgStart = lineStart = endPos + 1
      } else if (line.substring(0, 7) === 'binary:') {
        // handle binary responses "as-is" by forwarding
        // the expected amount of data and its trailing newline
        const binaryEnd = endPos + 1 + parseInt(line.substring(7), 10) + 1

        if (binaryEnd > buf.length) {
          if (lineStart > msgStart) {
            this._msgParts.push(buf.substring(msgStart, lineStart))
          }
          this._buf = buf.substring(lineStart)
          return
        }

        lineStart = binaryEnd
      } else {
        lineStart = endPos + 1
      }
    }

    if (lineStart > msgStart) {
      this._msgParts.push(buf.substring(msgStart, lineStart))
    }
    this._buf = buf.substring(lineStart)
  }

  _handleIdling (msg) {
    // store events and trigger with delay,
    // either a problem with MPD (not likely)
    // or this implementation; same events are
    // triggered multiple times (especially mixer)
    if (isNonEmptyString(msg)) {
      let msgs = msg.split('\n').filter(s => s.length > 9)
      for (let msg of msgs) {
        let name = msg.substring(9)
        this._idleevts[name] = true
      }
    }
    if (this._promiseQueue.length === 0) {
      this.idling = false
      this.setupIdling()
    }
    clearTimeout(this._idleevtsTID)
    this._idleevtsTID = setTimeout(this._triggerIdleEvents, 16)
  }

  _triggerIdleEvents () {
    for (let name in this._idleevts) {
      debug('triggering %s', name)
      this.emit(`system-${name}`)
      this.emit('system', name)
    }
    this._idleevts = {}
  }
}

/**
 * check that we're connected to MPD
 * and check for password requirements
 */
const finalizeClientConnection = (client, socket) =>
  new Promise((resolve, reject) => {
    socket.setEncoding('utf8')
    socket.on('error', reject)

    let protoVersion
    let idleCheckTimeout
    let password = isNonEmptyString(client._config.password)
      ? client._config.password
      : false

    const onTimeout = () => {
      debug('socket timed out')
      try {
        socket.destroy()
      } catch (e) {
        debug('socket destroy failed')
      }
      client.emit('close')
      reject(new MPDError('Connection timed out', 'CONNECTION_TIMEOUT'))
    }

    const finalize = () => {
      debug('preparing client')

      Object.defineProperty(
        client,
        'PROTOCOL_VERSION',
        { get: () => socket.destroyed ? undefined : protoVersion }
      )

      if (password) {
        delete client._config.password
      }

      socket.removeListener('data', onData)
      socket.removeListener('timeout', onTimeout)
      socket.on('data', client._receive)
      socket.on('close', () => {
        debug('close')
        client.emit('close')
      })

      client.socket = socket

      client.setupIdling()
      resolve(client)
    }

    const onData = data => {
      // expected MPD proto response
      if (!MPD_SENTINEL.test(data)) {
        debug('invalid server response %s', data)
        reject(new MPDError('Unexpected MPD service response',
          'INVALIDMPDSERVICE', `got: '${data}'`))
        return
      }

      // initial response with proto version
      if (OK_MPD.test(data) && !protoVersion) {
        protoVersion = data.split(OK_MPD)[1]
        debug('connected to MPD server, proto version: %s', protoVersion)
        // check for presence of the password
        if (password) {
          debug('sending password')
          socket.write(`password ${escapeArg(password)}\n`)
          return
        }
      }

      // check if there was an error (password / idle)
      const error = isError(data)
      if (error) {
        reject(error)
        socket.destroy()
        return
      }

      // do we need to test with the idle?
      if (!idleCheckTimeout) {
        debug('idle check')
        // set idle to test for the error for
        // in case MPD requires a password but
        // has not been set
        socket.write('idle\n')
        // idle does not respond, so if there
        // was no error, disable idle to get
        // the response
        idleCheckTimeout = setTimeout(() => {
          socket.write('noidle\n')
        }, 100)

        return
      }

      finalize()
    }

    socket.on('data', onData)
    socket.on('timeout', onTimeout)
  })

const getDefaultConfig = () => {
  const config = {}

  const timeout = Number(process.env.MPD_TIMEOUT)
  if (!Number.isNaN(timeout)) {
    config.timeout = timeout
  }

  const socket = [
    process.env.MPD_HOST,
    process.env.XDG_RUNTIME_DIR
      ? path.join(process.env.XDG_RUNTIME_DIR, 'mpd', 'socket')
      : undefined
  ].find(candidate => candidate ? isSocket(candidate) : false)

  if (socket) {
    config.path = socket
  } else {
    config.host = process.env.MPD_HOST || 'localhost'
    config.port = process.env.MPD_PORT || 6600
  }

  return config
}

const isSocket = socketPath => {
  if (typeof socketPath !== 'string' || socketPath.length === 0) {
    return
  }

  try {
    debug('default config: checking if %o is a socket', socketPath)
    if (fs.lstatSync(socketPath).isSocket()) {
      return socketPath
    }
  } catch (e) { }
}

MPDClient.MPDError = MPDError

MPDClient.Command = Command
MPDClient.cmd = Command.cmd

MPDClient.parseList = parseList
MPDClient.parseNestedList = parseNestedList
MPDClient.parseListAndAccumulate = parseListAndAccumulate
MPDClient.parseObject = parseObject

MPDClient.normalizeKeys = normalizeKeys
MPDClient.autoparseValues = autoparseValues

module.exports = MPDClient
module.exports.default = MPDClient
