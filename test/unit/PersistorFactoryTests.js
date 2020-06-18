const sinon = require('sinon')
const chai = require('chai')
const { expect } = chai
const SandboxedModule = require('sandboxed-module')

const modulePath = '../../src/PersistorFactory.js'

describe('PersistorManager', function () {
  let PersistorFactory, FSPersistor, S3Persistor, Settings

  beforeEach(function () {
    FSPersistor = {
      wrappedMethod: sinon.stub().returns('FSPersistor')
    }
    S3Persistor = {
      wrappedMethod: sinon.stub().returns('S3Persistor')
    }

    Settings = {}
    const requires = {
      './S3Persistor': S3Persistor,
      './FSPersistor': FSPersistor,
      './Settings': Settings,
      'logger-sharelatex': {
        info() {},
        err() {}
      }
    }
    PersistorFactory = SandboxedModule.require(modulePath, { requires })
  })

  it('should implement the S3 wrapped method when S3 is configured', function () {
    Settings.backend = 's3'

    expect(PersistorFactory()).to.respondTo('wrappedMethod')
    expect(PersistorFactory().wrappedMethod()).to.equal('S3Persistor')
  })

  it("should implement the S3 wrapped method when 'aws-sdk' is configured", function () {
    Settings.backend = 'aws-sdk'

    expect(PersistorFactory()).to.respondTo('wrappedMethod')
    expect(PersistorFactory().wrappedMethod()).to.equal('S3Persistor')
  })

  it('should implement the FS wrapped method when FS is configured', function () {
    Settings.backend = 'fs'

    expect(PersistorFactory()).to.respondTo('wrappedMethod')
    expect(PersistorFactory().wrappedMethod()).to.equal('FSPersistor')
  })

  it('should throw an error when the backend is not configured', function () {
    try {
      PersistorFactory()
    } catch (err) {
      expect(err.message).to.equal('no backend specified - config incomplete')
      return
    }
    expect('should have caught an error').not.to.exist
  })

  it('should throw an error when the backend is unknown', function () {
    Settings.backend = 'magic'
    try {
      PersistorFactory()
    } catch (err) {
      expect(err.message).to.equal('unknown filestore backend: magic')
      return
    }
    expect('should have caught an error').not.to.exist
  })
})
